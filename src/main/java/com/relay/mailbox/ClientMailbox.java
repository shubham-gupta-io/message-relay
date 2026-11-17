package com.relay.mailbox;

import com.relay.protocol.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory mailbox for a client.
 * <p>
 * - Maintains a pending queue (FIFO) and an in-flight map for unacknowledged messages.
 * - Uses a ReentrantLock per mailbox so operations on different clients do not block each other.
 * - Restores unacknowledged messages to the front of the queue on reconnect to preserve FIFO delivery.
 */
public class ClientMailbox {

    private static final Logger log = LoggerFactory.getLogger(ClientMailbox.class);

    private final String clientId;
    private final int maxCapacity;
    private final ReentrantLock lock = new ReentrantLock();

    // FIFO queue of pending messages awaiting delivery
    private final Deque<MessageRecord> pendingMessages = new ArrayDeque<>();

    // Map of delivered messages awaiting explicit ACK from recipient, ordered by dispatch time
    private final Map<String, MessageRecord> inFlightMessages = new LinkedHashMap<>();

    // Active connection channel to the client, if currently connected
    private DeliveryChannel activeChannel;

    // Optional callback for persistence / WAL updates on ACK: (clientId, msgId)
    private java.util.function.BiConsumer<String, String> ackCallback;

    public ClientMailbox(String clientId, int maxCapacity) {
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be null or blank");
        }
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity must be > 0: " + maxCapacity);
        }
        this.clientId = clientId;
        this.maxCapacity = maxCapacity;
    }

    public String clientId() {
        return clientId;
    }

    public int maxCapacity() {
        return maxCapacity;
    }

    /**
     * Enqueues a message for delivery to this client.
     * If the recipient is currently online, immediately triggers delivery.
     */
    public SendResult enqueue(MessageRecord message) {
        lock.lock();
        try {
            int currentSize = pendingMessages.size() + inFlightMessages.size();
            if (currentSize >= maxCapacity) {
                log.warn("Mailbox full for client '{}' (current={}, limit={}). Rejecting msgId='{}'",
                        clientId, currentSize, maxCapacity, message.msgId());
                return SendResult.reject(ErrorCode.MAILBOX_FULL);
            }

            pendingMessages.addLast(message);
            log.debug("Enqueued msgId='{}' to client '{}' pending queue. Pending count={}",
                    message.msgId(), clientId, pendingMessages.size());

            drainPendingLocked();
            return SendResult.accept();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attaches a new active delivery channel (e.g. upon client registration or reconnection).
     * <p>
     * Any superseded active channel is cleanly closed.
     * Any unacknowledged in-flight messages from prior connections are restored to the front
     * of the pending queue in their original dispatch order (preserving FIFO delivery).
     */
    public void attach(DeliveryChannel newChannel) {
        lock.lock();
        try {
            if (activeChannel != null && activeChannel != newChannel) {
                log.info("Client '{}' reconnected from a new channel. Closing superseded active channel.", clientId);
                activeChannel.closeQuietly();
            }

            // Restore unacknowledged in-flight messages to head of pending queue in original FIFO order
            restoreInFlightToPendingLocked();

            this.activeChannel = newChannel;
            drainPendingLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Detaches an active delivery channel upon client disconnect.
     * Moves any in-flight unacknowledged messages back to the front of the pending queue.
     */
    public void detach(DeliveryChannel channel) {
        lock.lock();
        try {
            if (this.activeChannel == channel) {
                this.activeChannel = null;
                restoreInFlightToPendingLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    private void restoreInFlightToPendingLocked() {
        if (!inFlightMessages.isEmpty()) {
            List<MessageRecord> unacked = new ArrayList<>(inFlightMessages.values());
            inFlightMessages.clear();
            // Prepend in reverse iteration so the earliest dispatched message remains
            // at the head of the queue (preserving original FIFO order upon reconnect)
            for (int i = unacked.size() - 1; i >= 0; i--) {
                pendingMessages.addFirst(unacked.get(i));
            }
            log.info("Restored {} unacknowledged messages for client '{}' to pending queue for redelivery.",
                    unacked.size(), clientId);
        }
    }

    /**
     * Sets a callback invoked when a message is successfully acknowledged.
     * Used for persistence (WAL) updates.
     */
    public void setAckCallback(java.util.function.BiConsumer<String, String> ackCallback) {
        lock.lock();
        try {
            this.ackCallback = ackCallback;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Explicitly acknowledges a message.
     * Messages are removed from the mailbox only upon receipt of this ACK.
     * Stale or repeated ACKs are treated as idempotent no-ops.
     *
     * @param msgId the message ID being acknowledged.
     * @return true if the message was found and removed, false if unknown or already acknowledged.
     */
    public boolean acknowledge(String msgId) {
        lock.lock();
        try {
            boolean removed = inFlightMessages.remove(msgId) != null;
            if (!removed) {
                // If not currently in-flight, check if a brief disconnect/reconnect blip
                // returned it to the pending queue right before this ACK arrived
                removed = pendingMessages.removeIf(m -> m.msgId().equals(msgId));
            }

            if (removed) {
                log.debug("Acknowledged and removed msgId='{}' for client '{}'", msgId, clientId);
                if (ackCallback != null) {
                    ackCallback.accept(clientId, msgId);
                }
                return true;
            }

            log.debug("Ignored stale or unknown ACK for msgId='{}' from client '{}'", msgId, clientId);
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drains pending messages to the active delivery channel in FIFO order.
     * Must be called while holding {@code lock}.
     */
    private void drainPendingLocked() {
        while (activeChannel != null && activeChannel.isOpen() && !pendingMessages.isEmpty()) {
            MessageRecord message = pendingMessages.pollFirst();
            inFlightMessages.put(message.msgId(), message);

            boolean delivered = activeChannel.deliver(message);
            if (!delivered) {
                log.warn("Failed to deliver msgId='{}' to client '{}'. Transport error, restoring to pending queue.",
                        message.msgId(), clientId);
                inFlightMessages.remove(message.msgId());
                pendingMessages.addFirst(message);
                activeChannel.closeQuietly();
                activeChannel = null;
                break;
            }
        }
    }

    public int pendingCount() {
        lock.lock();
        try {
            return pendingMessages.size();
        } finally {
            lock.unlock();
        }
    }

    public int inFlightCount() {
        lock.lock();
        try {
            return inFlightMessages.size();
        } finally {
            lock.unlock();
        }
    }

    public int totalCount() {
        lock.lock();
        try {
            return pendingMessages.size() + inFlightMessages.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks if the mailbox currently has an open, active delivery connection.
     */
    public boolean hasActiveChannel() {
        lock.lock();
        try {
            return activeChannel != null && activeChannel.isOpen();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns a snapshot of all active unacknowledged messages (in-flight + pending) in FIFO order.
     * Used for WAL checkpointing.
     */
    public List<MessageRecord> getAllActiveMessages() {
        lock.lock();
        try {
            List<MessageRecord> list = new ArrayList<>(inFlightMessages.size() + pendingMessages.size());
            list.addAll(inFlightMessages.values());
            list.addAll(pendingMessages);
            return list;
        } finally {
            lock.unlock();
        }
    }
}
