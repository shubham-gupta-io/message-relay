package com.relay.protocol;

/**
 * Standard error codes returned in ERROR and SEND_ACK frames.
 */
public final class ErrorCode {
    private ErrorCode() {}

    public static final String INVALID_FRAME = "INVALID_FRAME";
    public static final String UNREGISTERED = "UNREGISTERED";
    public static final String INVALID_FIELD = "INVALID_FIELD";
    public static final String PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE";
    public static final String CONNECTION_LIMIT_EXCEEDED = "CONNECTION_LIMIT_EXCEEDED";
    public static final String MAILBOX_FULL = "MAILBOX_FULL";
    public static final String DUPLICATE_MSG_ID = "DUPLICATE_MSG_ID";
    public static final String STORAGE_ERROR = "STORAGE_ERROR";
}
