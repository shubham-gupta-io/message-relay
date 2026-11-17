package com.relay.server;

import java.nio.file.Path;

/**
 * Configuration for the Relay Server.
 */
public record RelayConfig(
        int port,
        int maxActiveConnections,
        int maxMailboxCapacity,
        int maxFrameBytes,
        int socketTimeoutMs,
        Path dataDir
) {
    public static final int DEFAULT_PORT = 8080;
    public static final int DEFAULT_MAX_ACTIVE_CONNECTIONS = 100;
    public static final int DEFAULT_MAX_MAILBOX_CAPACITY = 1000;
    public static final int DEFAULT_MAX_FRAME_BYTES = 16384; // 16 KB
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 30_000; // 30 seconds

    public RelayConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535: " + port);
        }
        if (maxActiveConnections <= 0) {
            throw new IllegalArgumentException("maxActiveConnections must be > 0: " + maxActiveConnections);
        }
        if (maxMailboxCapacity <= 0) {
            throw new IllegalArgumentException("maxMailboxCapacity must be > 0: " + maxMailboxCapacity);
        }
        if (maxFrameBytes <= 0) {
            throw new IllegalArgumentException("maxFrameBytes must be > 0: " + maxFrameBytes);
        }
        if (socketTimeoutMs < 0) {
            throw new IllegalArgumentException("socketTimeoutMs must be >= 0: " + socketTimeoutMs);
        }
    }

    public static RelayConfig defaultConfig() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int port = DEFAULT_PORT;
        private int maxActiveConnections = DEFAULT_MAX_ACTIVE_CONNECTIONS;
        private int maxMailboxCapacity = DEFAULT_MAX_MAILBOX_CAPACITY;
        private int maxFrameBytes = DEFAULT_MAX_FRAME_BYTES;
        private int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
        private Path dataDir = null;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder maxActiveConnections(int maxActiveConnections) {
            this.maxActiveConnections = maxActiveConnections;
            return this;
        }

        public Builder maxMailboxCapacity(int maxMailboxCapacity) {
            this.maxMailboxCapacity = maxMailboxCapacity;
            return this;
        }

        public Builder maxFrameBytes(int maxFrameBytes) {
            this.maxFrameBytes = maxFrameBytes;
            return this;
        }

        public Builder socketTimeoutMs(int socketTimeoutMs) {
            this.socketTimeoutMs = socketTimeoutMs;
            return this;
        }

        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public RelayConfig build() {
            return new RelayConfig(port, maxActiveConnections, maxMailboxCapacity, maxFrameBytes, socketTimeoutMs, dataDir);
        }
    }
}
