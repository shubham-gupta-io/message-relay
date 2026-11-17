package com.relay.mailbox;

public record MessageRecord(
        String msgId,
        String senderId,
        String recipientId,
        String payload,
        long timestamp
) {
    public MessageRecord(String msgId, String senderId, String recipientId, String payload) {
        this(msgId, senderId, recipientId, payload, System.currentTimeMillis());
    }
}
