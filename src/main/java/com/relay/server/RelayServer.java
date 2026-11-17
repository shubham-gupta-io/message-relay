package com.relay.server;

import com.relay.framing.BoundedFrameWriter;
import com.relay.mailbox.ClientMailbox;
import com.relay.mailbox.MessageRecord;
import com.relay.mailbox.SendResult;
import com.relay.protocol.ErrorCode;
import com.relay.protocol.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP Message Relay Server using Java 21 Virtual Threads.
 * Manages client connections, mailboxes, and server lifecycle.
 */
public class RelayServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RelayServer.class);

    private final RelayConfig config;
    private final ExecutorService executor;
    private final Semaphore connectionLimiter;
    private final ConcurrentMap<String, ClientMailbox> mailboxes;
    private final Set<ClientConnection> activeConnections;
    private final MessageDeduplicator deduplicator;
    private final WalStorage walStorage;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ServerSocket serverSocket;
    private Thread shutdownHook;
    private int boundPort;

    public RelayServer() {
        this(RelayConfig.defaultConfig());
    }

    public RelayServer(RelayConfig config) {
        this.config = config;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.connectionLimiter = new Semaphore(config.maxActiveConnections(), true);
        this.mailboxes = new ConcurrentHashMap<>();
        this.activeConnections = ConcurrentHashMap.newKeySet();
        this.deduplicator = new MessageDeduplicator();
        this.walStorage = (config.dataDir() != null) ? new WalStorage(config.dataDir()) : null;
    }

    public void start() throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("Server has already been closed");
        }
        if (running.compareAndSet(false, true)) {
            // Restore persistent state from WAL on startup if enabled
            if (walStorage != null) {
                Map<String, List<MessageRecord>> recovered = walStorage.recover();
                for (Map.Entry<String, List<MessageRecord>> entry : recovered.entrySet()) {
                    ClientMailbox mb = mailboxes.computeIfAbsent(entry.getKey(), this::createMailbox);
                    for (MessageRecord msg : entry.getValue()) {
                        mb.enqueue(msg);
                    }
                }
            }

            this.serverSocket = new ServerSocket(config.port());
            this.boundPort = serverSocket.getLocalPort();

            // Register shutdown hook
            this.shutdownHook = new Thread(this::close, "relay-server-shutdown-hook");
            try {
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down
            }

            log.info("RelayServer started on port {} (maxConn={}, maxMailbox={}, maxFrameBytes={}, persistence={})",
                    boundPort, config.maxActiveConnections(), config.maxMailboxCapacity(), config.maxFrameBytes(),
                    (walStorage != null ? "WAL enabled" : "in-memory"));

            executor.submit(this::acceptLoop);
        }
    }

    private void acceptLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (!running.get() || serverSocket.isClosed()) {
                    break;
                }
                log.warn("Error accepting client socket: {}", e.getMessage());
                continue;
            }

            // Enforce connection limit to guard against resource exhaustion
            if (!connectionLimiter.tryAcquire()) {
                rejectOverLimit(socket);
                continue;
            }

            handleAcceptedSocket(socket);
        }
    }

    private void rejectOverLimit(Socket socket) {
        log.warn("Connection limit ({}) reached. Rejecting connection from {}",
                config.maxActiveConnections(), socket.getRemoteSocketAddress());
        try (socket; BoundedFrameWriter writer = new BoundedFrameWriter(socket.getOutputStream())) {
            writer.writeFrame(new Frame.Error(
                    ErrorCode.CONNECTION_LIMIT_EXCEEDED,
                    "Maximum active connections limit (" + config.maxActiveConnections() + ") exceeded"
            ));
        } catch (IOException ignored) {}
    }

    private void handleAcceptedSocket(Socket socket) {
        try {
            ClientConnection connection = new ClientConnection(socket, this, config);
            activeConnections.add(connection);
            // Run connection handler on a lightweight Virtual Thread (one per client)
            executor.submit(() -> {
                try {
                    connection.run();
                } finally {
                    connectionLimiter.release();
                }
            });
        } catch (IOException e) {
            connectionLimiter.release();
            try {
                socket.close();
            } catch (IOException ignored) {}
            log.warn("Failed to initialize connection handler for {}: {}",
                    socket.getRemoteSocketAddress(), e.getMessage());
        }
    }

    private ClientMailbox createMailbox(String clientId) {
        ClientMailbox mb = new ClientMailbox(clientId, config.maxMailboxCapacity());
        if (walStorage != null) {
            mb.setAckCallback(walStorage::recordAck);
        }
        return mb;
    }

    public void registerClient(String clientId, ClientConnection connection) {
        ClientMailbox mailbox = mailboxes.computeIfAbsent(clientId, this::createMailbox);
        connection.setMailbox(mailbox);
        mailbox.attach(connection);
    }

    public SendResult sendToMailbox(String senderId, String recipientId, String msgId, String payload) {
        // Ensure recipient mailbox exists even if the recipient is currently offline
        ClientMailbox recipientMailbox = mailboxes.computeIfAbsent(recipientId, this::createMailbox);
        MessageRecord record = new MessageRecord(msgId, senderId, recipientId, payload);

        // Write-Ahead Logging: write to disk before in-memory queueing to ensure crash resilience
        if (walStorage != null) {
            try {
                walStorage.recordEnqueue(record);
            } catch (UncheckedIOException e) {
                log.error("Storage failure recording message: {}", e.getMessage());
                return SendResult.reject(ErrorCode.STORAGE_ERROR);
            }
        }

        return recipientMailbox.enqueue(record);
    }

    public void onConnectionClosed(ClientConnection connection) {
        activeConnections.remove(connection);
        log.debug("Active connection closed for client '{}'. Remaining connections={}",
                connection.registeredClientId(), activeConnections.size());
    }

    public MessageDeduplicator deduplicator() {
        return deduplicator;
    }

    public ClientMailbox getMailbox(String clientId) {
        return mailboxes.get(clientId);
    }

    public int getPort() {
        return boundPort;
    }

    public int getActiveConnectionCount() {
        return activeConnections.size();
    }

    public RelayConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            running.set(false);
            log.info("Shutting down RelayServer on port {}...", boundPort);

            // Remove shutdown hook if called programmatically
            if (shutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM already shutting down
                }
            }

            // Close server listening socket
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    log.warn("Error closing server socket: {}", e.getMessage());
                }
            }

            // Cleanly close all active client connections
            for (ClientConnection connection : activeConnections) {
                try {
                    connection.close();
                } catch (Exception e) {
                    log.warn("Error closing client connection: {}", e.getMessage());
                }
            }
            activeConnections.clear();

            // Perform WAL compaction / checkpoint on clean shutdown
            if (walStorage != null) {
                walStorage.checkpoint(mailboxes);
            }

            // Shutdown virtual thread executor
            executor.shutdown();
            try {
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            log.info("RelayServer shutdown complete.");
        }
    }
}
