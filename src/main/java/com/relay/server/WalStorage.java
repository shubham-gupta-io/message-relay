package com.relay.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.relay.mailbox.ClientMailbox;
import com.relay.mailbox.MessageRecord;
import com.relay.protocol.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead Log (WAL) for state persistence and crash recovery.
 */
public class WalStorage {

    private static final Logger log = LoggerFactory.getLogger(WalStorage.class);
    private static final String WAL_FILENAME = "wal.log";
    private static final String TMP_FILENAME = "wal.log.tmp";

    private final Path dataDir;
    private final Path walPath;
    private final ObjectMapper mapper;
    private final ReentrantLock lock = new ReentrantLock();

    public WalStorage(Path dataDir) {
        this.dataDir = dataDir;
        this.walPath = dataDir.resolve(WAL_FILENAME);
        this.mapper = JsonCodec.mapper();
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create storage directory: " + dataDir, e);
        }
    }

    public void recordEnqueue(MessageRecord message) {
        ObjectNode root = mapper.createObjectNode();
        root.put("op", "ENQUEUE");
        root.set("message", mapper.valueToTree(message));
        appendOp(root, "Failed to append ENQUEUE to WAL for msgId='" + message.msgId() + "'");
    }

    public void recordAck(String clientId, String msgId) {
        ObjectNode root = mapper.createObjectNode();
        root.put("op", "ACK");
        root.put("clientId", clientId);
        root.put("msgId", msgId);
        appendOp(root, "Failed to append ACK to WAL for clientId='" + clientId + "', msgId='" + msgId + "'");
    }

    private void appendOp(ObjectNode root, String errorContext) {
        lock.lock();
        try {
            appendLine(mapper.writeValueAsString(root));
        } catch (IOException e) {
            log.error("{}: {}", errorContext, e.getMessage());
            throw new UncheckedIOException("WAL write failed", e);
        } finally {
            lock.unlock();
        }
    }

    private void appendLine(String line) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                walPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            writer.write(line);
            writer.newLine();
            writer.flush();
        }
    }

    /**
     * Recovers active mailboxes by sequentially replaying the WAL from top to bottom.
     * An ENQUEUE adds a message to the recipient's map; an ACK removes it.
     * Surviving entries represent the exact unacknowledged messages in FIFO order.
     */
    public Map<String, List<MessageRecord>> recover() {
        lock.lock();
        try {
            if (!Files.exists(walPath)) {
                return Map.of();
            }

            Map<String, LinkedHashMap<String, MessageRecord>> recovered = new LinkedHashMap<>();
            try (BufferedReader reader = Files.newBufferedReader(walPath, StandardCharsets.UTF_8)) {
                String line;
                int lineNumber = 0;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (!line.isBlank()) {
                        applyWalLine(line, recovered, lineNumber);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read WAL for recovery: " + walPath, e);
            }

            Map<String, List<MessageRecord>> result = new LinkedHashMap<>();
            for (Map.Entry<String, LinkedHashMap<String, MessageRecord>> entry : recovered.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    result.put(entry.getKey(), new ArrayList<>(entry.getValue().values()));
                }
            }
            log.info("WAL recovery complete: restored {} mailboxes from disk.", result.size());
            return result;
        } finally {
            lock.unlock();
        }
    }

    private void applyWalLine(String line, Map<String, LinkedHashMap<String, MessageRecord>> recovered, int lineNumber) {
        try {
            JsonNode node = mapper.readTree(line);
            String op = node.path("op").asText();
            if ("ENQUEUE".equals(op)) {
                MessageRecord msg = mapper.treeToValue(node.get("message"), MessageRecord.class);
                recovered.computeIfAbsent(msg.recipientId(), k -> new LinkedHashMap<>())
                        .put(msg.msgId(), msg);
            } else if ("ACK".equals(op)) {
                String clientId = node.path("clientId").asText();
                String msgId = node.path("msgId").asText();
                LinkedHashMap<String, MessageRecord> clientMap = recovered.get(clientId);
                if (clientMap != null) {
                    clientMap.remove(msgId);
                }
            }
        } catch (Exception e) {
            log.warn("Corrupted WAL entry at line {}: {}. Skipping.", lineNumber, e.getMessage());
        }
    }

    /**
     * Compacts the WAL on shutdown by writing only active, unacknowledged messages.
     * Uses atomic file replacement (wal.log.tmp -> wal.log) to guarantee the log
     * is never left in a corrupted or half-written state if a crash occurs midway.
     */
    public void checkpoint(Map<String, ClientMailbox> mailboxes) {
        lock.lock();
        try {
            Path tmpPath = dataDir.resolve(TMP_FILENAME);
            try (BufferedWriter writer = Files.newBufferedWriter(
                    tmpPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                for (ClientMailbox mailbox : mailboxes.values()) {
                    for (MessageRecord msg : mailbox.getAllActiveMessages()) {
                        ObjectNode root = mapper.createObjectNode();
                        root.put("op", "ENQUEUE");
                        root.set("message", mapper.valueToTree(msg));
                        writer.write(mapper.writeValueAsString(root));
                        writer.newLine();
                    }
                }
                writer.flush();
            }

            Files.move(tmpPath, walPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log.info("WAL checkpoint complete. State compacted cleanly.");
        } catch (IOException e) {
            log.error("Failed to perform WAL checkpoint: {}", e.getMessage(), e);
        } finally {
            lock.unlock();
        }
    }
}
