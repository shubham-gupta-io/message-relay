package com.relay.mailbox;

public record SendResult(
        boolean accepted,
        String reason
) {
    public static SendResult accept() {
        return new SendResult(true, null);
    }

    public static SendResult reject(String reason) {
        return new SendResult(false, reason);
    }
}
