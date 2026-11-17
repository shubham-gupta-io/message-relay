package com.relay;

import com.relay.server.RelayConfig;
import com.relay.server.RelayServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Message Relay Server.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        int port = RelayConfig.DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0] + ". Defaulting to " + port);
            }
        } else if (System.getenv("PORT") != null) {
            try {
                port = Integer.parseInt(System.getenv("PORT"));
            } catch (NumberFormatException ignored) {}
        }

        java.nio.file.Path dataDir = null;
        if (args.length > 1) {
            dataDir = java.nio.file.Path.of(args[1]);
        } else if (System.getenv("DATA_DIR") != null) {
            dataDir = java.nio.file.Path.of(System.getenv("DATA_DIR"));
        }

        RelayConfig config = RelayConfig.builder()
                .port(port)
                .dataDir(dataDir)
                .build();

        RelayServer server = new RelayServer(config);
        try {
            server.start();
            log.info("Relay Server is ready to receive connections on port {}", server.getPort());

            // Block main thread until JVM termination
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            log.info("Main thread interrupted, shutting down server.");
            server.close();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Fatal error starting RelayServer: {}", e.getMessage(), e);
            server.close();
            System.exit(1);
        }
    }
}
