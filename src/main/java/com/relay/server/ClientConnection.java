package com.relay.server;

import com.relay.framing.BoundedFrameReader;
import com.relay.framing.BoundedFrameWriter;
import com.relay.framing.PayloadTooLargeException;
import com.relay.mailbox.ClientMailbox;
import com.relay.mailbox.DeliveryChannel;
import com.relay.mailbox.MessageRecord;
import com.relay.mailbox.SendResult;
import com.relay.protocol.ErrorCode;
import com.relay.protocol.Frame;
import com.relay.protocol.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles communication with a single TCP client connection on a dedicated Virtual Thread.
 */
public class ClientConnection implements DeliveryChannel, Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);

    private final Socket socket;
    private final RelayServer server;
    private final BoundedFrameReader reader;
    private final BoundedFrameWriter writer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile String registeredClientId;
    private volatile ClientMailbox mailbox;

    public ClientConnection(Socket socket, RelayServer server, RelayConfig config) throws IOException {
        this.socket = socket;
        this.server = server;

        socket.setSoTimeout(config.socketTimeoutMs());
        socket.setTcpNoDelay(true);

        this.reader = new BoundedFrameReader(socket.getInputStream(), config.maxFrameBytes());
        this.writer = new BoundedFrameWriter(socket.getOutputStream());
    }

    public String registeredClientId() {
        return registeredClientId;
    }

    public void setMailbox(ClientMailbox mailbox) {
        this.mailbox = mailbox;
    }

    @Override
    public boolean deliver(MessageRecord message) {
        if (closed.get()) {
            return false;
        }
        try {
            writer.writeFrame(new Frame.Deliver(
                    message.msgId(),
                    message.senderId(),
                    message.payload()
            ));
            log.debug("Pushed DELIVER msgId='{}' to client '{}'", message.msgId(), registeredClientId);
            return true;
        } catch (IOException e) {
            log.warn("Failed to deliver msgId='{}' to client '{}': {}",
                    message.msgId(), registeredClientId, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean isOpen() {
        return !closed.get() && !socket.isClosed();
    }

    @Override
    public void closeQuietly() {
        close();
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.debug("Closing connection for clientId='{}'", registeredClientId);
            try {
                if (mailbox != null) {
                    mailbox.detach(this);
                }
            } catch (Exception e) {
                log.warn("Error detaching mailbox for client '{}': {}", registeredClientId, e.getMessage());
            }

            try { reader.close(); } catch (Exception ignored) {}
            try { writer.close(); } catch (Exception ignored) {}
            try { socket.close(); } catch (Exception ignored) {}
        }
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                String line;
                try {
                    line = reader.readNextFrame();
                } catch (PayloadTooLargeException e) {
                    log.warn("Client exceeded frame limit: {}", e.getMessage());
                    sendErrorQuietly(ErrorCode.PAYLOAD_TOO_LARGE, e.getMessage());
                    break;
                } catch (SocketTimeoutException e) {
                    log.debug("Socket timeout on connection for client '{}'", registeredClientId);
                    break;
                } catch (IOException e) {
                    // Client disconnected or socket closed
                    break;
                }

                if (line == null) {
                    log.debug("Client closed connection cleanly (EOF)");
                    break;
                }

                if (line.isBlank()) {
                    continue;
                }

                handleRawFrame(line);
            }
        } finally {
            close();
            server.onConnectionClosed(this);
        }
    }

    private void handleRawFrame(String rawJson) {
        Frame frame;
        try {
            frame = JsonCodec.deserialize(rawJson);
        } catch (Exception e) {
            log.warn("Received malformed JSON frame: '{}'", rawJson);
            sendErrorQuietly(ErrorCode.INVALID_FRAME, "Malformed JSON: " + e.getMessage());
            return;
        }

        if (frame == null) {
            sendErrorQuietly(ErrorCode.INVALID_FRAME, "Frame cannot be null");
            return;
        }

        try {
            handleFrame(frame);
        } catch (IOException e) {
            log.warn("I/O error handling frame for client '{}': {}", registeredClientId, e.getMessage());
            close();
        }
    }

    private void handleFrame(Frame frame) throws IOException {
        switch (frame) {
            case Frame.Register reg -> handleRegister(reg);
            case Frame.Send send -> handleSend(send);
            case Frame.Ack ack -> handleAck(ack);
            case Frame.Error err -> log.warn("Received ERROR frame from client '{}': [{}] {}",
                    registeredClientId, err.code(), err.message());
            case Frame.Registered f -> rejectServerFrame(f.type());
            case Frame.SendAck f -> rejectServerFrame(f.type());
            case Frame.Deliver f -> rejectServerFrame(f.type());
        }
    }

    private void rejectServerFrame(String frameType) throws IOException {
        writer.writeFrame(new Frame.Error(
                ErrorCode.INVALID_FRAME,
                "Client sent unexpected server-to-client frame: " + frameType
        ));
    }

    private void sendErrorQuietly(String code, String message) {
        try {
            writer.writeFrame(new Frame.Error(code, message));
        } catch (IOException ignored) {}
    }

    private void handleRegister(Frame.Register reg) throws IOException {
        if (reg.clientId() == null || reg.clientId().isBlank()) {
            writer.writeFrame(new Frame.Error(ErrorCode.INVALID_FIELD, "clientId must not be null or blank"));
            return;
        }

        this.registeredClientId = reg.clientId().trim();
        server.registerClient(this.registeredClientId, this);
        writer.writeFrame(new Frame.Registered(this.registeredClientId));
        log.info("Client registered successfully: '{}'", this.registeredClientId);
    }

    private void handleSend(Frame.Send send) throws IOException {
        if (registeredClientId == null) {
            writer.writeFrame(new Frame.Error(ErrorCode.UNREGISTERED, "Client must REGISTER before sending messages"));
            return;
        }

        if (send.msgId() == null || send.msgId().isBlank()) {
            writer.writeFrame(new Frame.Error(ErrorCode.INVALID_FIELD, "msgId must not be null or blank"));
            return;
        }

        if (send.recipientId() == null || send.recipientId().isBlank()) {
            writer.writeFrame(Frame.SendAck.rejected(send.msgId(), ErrorCode.INVALID_FIELD));
            return;
        }

        if (send.payload() == null) {
            writer.writeFrame(Frame.SendAck.rejected(send.msgId(), ErrorCode.INVALID_FIELD));
            return;
        }

        // Check duplicate msgId from the same sender
        boolean isNew = server.deduplicator().checkAndRegister(registeredClientId, send.msgId());
        if (!isNew) {
            log.warn("Duplicate msgId='{}' from sender '{}'. Rejecting.", send.msgId(), registeredClientId);
            writer.writeFrame(Frame.SendAck.rejected(send.msgId(), ErrorCode.DUPLICATE_MSG_ID));
            return;
        }

        // Deliver or enqueue into recipient's mailbox
        SendResult result = server.sendToMailbox(
                registeredClientId,
                send.recipientId().trim(),
                send.msgId(),
                send.payload()
        );

        if (result.accepted()) {
            writer.writeFrame(Frame.SendAck.accepted(send.msgId()));
        } else {
            writer.writeFrame(Frame.SendAck.rejected(send.msgId(), result.reason()));
        }
    }

    private void handleAck(Frame.Ack ack) throws IOException {
        if (registeredClientId == null) {
            writer.writeFrame(new Frame.Error(ErrorCode.UNREGISTERED, "Client must REGISTER before sending ACKs"));
            return;
        }

        if (ack.msgId() == null || ack.msgId().isBlank()) {
            return;
        }

        if (mailbox != null) {
            boolean acked = mailbox.acknowledge(ack.msgId());
            if (!acked) {
                log.debug("ACK for msgId='{}' was not in flight (stale or duplicate)", ack.msgId());
            }
        }
    }
}
