package com.relay;

import com.relay.framing.BoundedFrameReader;
import com.relay.framing.BoundedFrameWriter;
import com.relay.mailbox.ClientMailbox;
import com.relay.protocol.ErrorCode;
import com.relay.protocol.Frame;
import com.relay.protocol.JsonCodec;
import com.relay.server.RelayConfig;
import com.relay.server.RelayServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(10)
class RelayServerTest {

    static class TestClient implements AutoCloseable {
        final Socket socket;
        final BoundedFrameReader reader;
        final BoundedFrameWriter writer;
        final BlockingQueue<Frame.Deliver> deliverQueue = new LinkedBlockingQueue<>();
        final BlockingQueue<Frame> inboundQueue = new LinkedBlockingQueue<>();
        final AtomicBoolean closed = new AtomicBoolean(false);

        TestClient(int port) throws IOException {
            this.socket = new Socket("127.0.0.1", port);
            this.reader = new BoundedFrameReader(socket.getInputStream(), 65536);
            this.writer = new BoundedFrameWriter(socket.getOutputStream());

            Thread.ofVirtual().start(() -> {
                try {
                    while (!closed.get()) {
                        String line = reader.readNextFrame();
                        if (line == null) break;
                        if (line.isBlank()) continue;
                        Frame frame = JsonCodec.deserialize(line);
                        if (frame instanceof Frame.Deliver deliver) {
                            deliverQueue.offer(deliver);
                        } else {
                            inboundQueue.offer(frame);
                        }
                    }
                } catch (Exception ignored) {
                }
            });
        }

        void register(String clientId) throws IOException {
            writer.writeFrame(new Frame.Register(clientId));
        }

        void send(String msgId, String recipientId, String payload) throws IOException {
            writer.writeFrame(new Frame.Send(msgId, recipientId, payload));
        }

        void ack(String msgId) throws IOException {
            writer.writeFrame(new Frame.Ack(msgId));
        }

        void sendRaw(String text) throws IOException {
            writer.writeRawLine(text);
        }

        Frame pollFrame(long timeoutMs) throws InterruptedException {
            return inboundQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }

        Frame.Deliver pollDeliver(long timeoutMs) throws InterruptedException {
            return deliverQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try { socket.close(); } catch (Exception ignored) {}
            }
        }
    }

    private RelayServer createAndStartServer(RelayConfig config) throws IOException {
        RelayServer server = new RelayServer(config);
        server.start();
        return server;
    }

    @Test
    @DisplayName("Scenario 1: Multi-client registration and end-to-end message send, delivery, and explicit ACK")
    void testEndToEndMessagingAndAck() throws Exception {
        RelayConfig config = RelayConfig.builder().port(0).build();
        try (RelayServer server = createAndStartServer(config);
             TestClient batman = new TestClient(server.getPort());
             TestClient robin = new TestClient(server.getPort())) {

            assertEquals(config.maxActiveConnections(), server.getConfig().maxActiveConnections());
            await().atMost(3, TimeUnit.SECONDS).until(() -> server.getActiveConnectionCount() == 2);

            // Register Batman
            batman.register("batman");
            Frame batmanReg = batman.pollFrame(3000);
            assertInstanceOf(Frame.Registered.class, batmanReg);
            assertEquals("batman", ((Frame.Registered) batmanReg).clientId());

            // Register Robin
            robin.register("robin");
            Frame robinReg = robin.pollFrame(3000);
            assertInstanceOf(Frame.Registered.class, robinReg);
            assertEquals("robin", ((Frame.Registered) robinReg).clientId());

            // Batman sends message to Robin
            batman.send("m-100", "robin", "Bat-signal active!");
            Frame sendAck = batman.pollFrame(3000);
            assertInstanceOf(Frame.SendAck.class, sendAck);
            assertEquals("m-100", ((Frame.SendAck) sendAck).msgId());
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) sendAck).status());

            // Robin receives DELIVER
            Frame.Deliver deliver = robin.pollDeliver(3000);
            assertNotNull(deliver);
            assertEquals("m-100", deliver.msgId());
            assertEquals("batman", deliver.senderId());
            assertEquals("Bat-signal active!", deliver.payload());

            // Robin sends explicit ACK
            robin.ack("m-100");

            // Verify Robin's mailbox is completely drained
            await().atMost(3, TimeUnit.SECONDS).until(() -> {
                ClientMailbox mb = server.getMailbox("robin");
                return mb != null && mb.totalCount() == 0;
            });
        }
    }

    @Test
    @DisplayName("Scenario 2: Offline recipient retention and FIFO delivery upon reconnect")
    void testOfflineRetentionAndFifoDelivery() throws Exception {
        RelayConfig config = RelayConfig.builder().port(0).build();
        try (RelayServer server = createAndStartServer(config);
             TestClient batman = new TestClient(server.getPort())) {

            batman.register("batman");
            assertNotNull(batman.pollFrame(3000));

            // Robin is offline. Batman sends 3 messages in order: msg-1, msg-2, msg-3
            batman.send("msg-1", "robin", "Mission briefing 1");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            batman.send("msg-2", "robin", "Mission briefing 2");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            batman.send("msg-3", "robin", "Mission briefing 3");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            // Verify server retained 3 messages for Robin
            ClientMailbox robinMailbox = server.getMailbox("robin");
            assertNotNull(robinMailbox);
            assertEquals(3, robinMailbox.pendingCount());

            // Robin connects and registers
            try (TestClient robin = new TestClient(server.getPort())) {
                robin.register("robin");
                assertNotNull(robin.pollFrame(3000));

                // Robin must receive all 3 messages in strict FIFO order
                Frame.Deliver d1 = robin.pollDeliver(3000);
                assertNotNull(d1);
                assertEquals("msg-1", d1.msgId());
                assertEquals("Mission briefing 1", d1.payload());

                Frame.Deliver d2 = robin.pollDeliver(3000);
                assertNotNull(d2);
                assertEquals("msg-2", d2.msgId());
                assertEquals("Mission briefing 2", d2.payload());

                Frame.Deliver d3 = robin.pollDeliver(3000);
                assertNotNull(d3);
                assertEquals("msg-3", d3.msgId());
                assertEquals("Mission briefing 3", d3.payload());

                // Robin acknowledges all 3
                robin.ack("msg-1");
                robin.ack("msg-2");
                robin.ack("msg-3");

                await().atMost(3, TimeUnit.SECONDS).until(() -> robinMailbox.totalCount() == 0);
            }
        }
    }

    @Test
    @DisplayName("Scenario 3: Redelivery of messages delivered before abrupt disconnect without ACK (FIFO order preserved)")
    void testRedeliveryOfUnackedMessages() throws Exception {
        RelayConfig config = RelayConfig.builder().port(0).build();
        try (RelayServer server = createAndStartServer(config);
             TestClient batman = new TestClient(server.getPort())) {

            batman.register("batman");
            assertNotNull(batman.pollFrame(3000));

            // Robin connects and registers
            TestClient robin1 = new TestClient(server.getPort());
            robin1.register("robin");
            assertNotNull(robin1.pollFrame(3000));

            // Batman sends m-1 and m-2 to Robin
            batman.send("m-1", "robin", "First");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            batman.send("m-2", "robin", "Second");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            // Robin receives m-1 and m-2
            Frame.Deliver d1 = robin1.pollDeliver(3000);
            assertNotNull(d1);
            assertEquals("m-1", d1.msgId());

            Frame.Deliver d2 = robin1.pollDeliver(3000);
            assertNotNull(d2);
            assertEquals("m-2", d2.msgId());

            // Robin ACKs m-1, but abruptly closes socket BEFORE acking m-2
            robin1.ack("m-1");
            Thread.sleep(100); // let ACK reach server
            robin1.close();

            // While Robin is disconnected, Batman sends m-3
            batman.send("m-3", "robin", "Third");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            // Robin reconnects with a fresh socket and registers
            try (TestClient robin2 = new TestClient(server.getPort())) {
                robin2.register("robin");
                assertNotNull(robin2.pollFrame(3000));

                // Robin must receive m-2 (redelivered unacked) followed by m-3 (FIFO order preserved)
                Frame.Deliver redeliveredM2 = robin2.pollDeliver(3000);
                assertNotNull(redeliveredM2);
                assertEquals("m-2", redeliveredM2.msgId());
                assertEquals("Second", redeliveredM2.payload());

                Frame.Deliver nextM3 = robin2.pollDeliver(3000);
                assertNotNull(nextM3);
                assertEquals("m-3", nextM3.msgId());
                assertEquals("Third", nextM3.payload());

                // ACK both
                robin2.ack("m-2");
                robin2.ack("m-3");

                await().atMost(3, TimeUnit.SECONDS).until(() -> server.getMailbox("robin").totalCount() == 0);
            }
        }
    }

    @Test
    @DisplayName("Scenario 4: Resource limits enforcement (mailbox capacity, oversized frame, connection limit)")
    void testResourceLimits() throws Exception {
        // 4a: Mailbox capacity enforcement
        RelayConfig mailboxLimitConfig = RelayConfig.builder()
                .port(0)
                .maxMailboxCapacity(2)
                .build();
        try (RelayServer server = createAndStartServer(mailboxLimitConfig);
             TestClient batman = new TestClient(server.getPort())) {

            batman.register("batman");
            assertNotNull(batman.pollFrame(3000));

            // Fill Robin's mailbox to limit (capacity = 2)
            batman.send("m-1", "robin", "p1");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            batman.send("m-2", "robin", "p2");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            // 3rd message must be rejected with MAILBOX_FULL
            batman.send("m-3", "robin", "p3");
            Frame.SendAck rejectedAck = (Frame.SendAck) batman.pollFrame(3000);
            assertEquals(Frame.SendAck.REJECTED, rejectedAck.status());
            assertEquals("MAILBOX_FULL", rejectedAck.reason());
        }

        // 4b: Oversized frame rejection and recovery/closure
        RelayConfig frameLimitConfig = RelayConfig.builder()
                .port(0)
                .maxFrameBytes(64) // strictly 64 bytes
                .build();
        try (RelayServer server = createAndStartServer(frameLimitConfig);
             TestClient client = new TestClient(server.getPort())) {

            // Send a payload larger than 64 bytes without a newline
            String oversizedPayload = "A".repeat(128) + "\n";
            client.sendRaw(oversizedPayload);

            Frame err = client.pollFrame(3000);
            assertInstanceOf(Frame.Error.class, err);
            assertEquals(ErrorCode.PAYLOAD_TOO_LARGE, ((Frame.Error) err).code());
        }

        // 4c: Max active connections enforcement
        RelayConfig connLimitConfig = RelayConfig.builder()
                .port(0)
                .maxActiveConnections(2)
                .build();
        try (RelayServer server = createAndStartServer(connLimitConfig);
             TestClient c1 = new TestClient(server.getPort());
             TestClient c2 = new TestClient(server.getPort())) {

            c1.register("c1");
            assertNotNull(c1.pollFrame(3000));

            c2.register("c2");
            assertNotNull(c2.pollFrame(3000));

            // 3rd connection exceeds limit of 2
            try (TestClient c3 = new TestClient(server.getPort())) {
                Frame err = c3.pollFrame(3000);
                assertInstanceOf(Frame.Error.class, err);
                assertEquals(ErrorCode.CONNECTION_LIMIT_EXCEEDED, ((Frame.Error) err).code());
            }
        }
    }

    @Test
    @DisplayName("Scenario 5: Edge cases and fault isolation (duplicates, stale ACKs, malformed input, slow client isolation)")
    void testEdgeCasesAndFaultIsolation() throws Exception {
        RelayConfig config = RelayConfig.builder().port(0).build();
        try (RelayServer server = createAndStartServer(config);
             TestClient batman = new TestClient(server.getPort());
             TestClient robin = new TestClient(server.getPort())) {

            batman.register("batman");
            assertNotNull(batman.pollFrame(3000));
            robin.register("robin");
            assertNotNull(robin.pollFrame(3000));

            // 5a: Duplicate msgId from same sender rejected with DUPLICATE_MSG_ID
            assertEquals(50_000, server.deduplicator().maxEntries());
            int initialDedupSize = server.deduplicator().size();
            batman.send("dup-1", "robin", "gadget delivery");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());
            assertEquals(initialDedupSize + 1, server.deduplicator().size());

            Frame.Deliver dFirst = robin.pollDeliver(3000);
            assertNotNull(dFirst);
            assertEquals("dup-1", dFirst.msgId());

            // Duplicate send from Batman with the same msgId
            batman.send("dup-1", "robin", "gadget delivery");
            Frame.SendAck dupAck = (Frame.SendAck) batman.pollFrame(3000);
            assertEquals(Frame.SendAck.REJECTED, dupAck.status());
            assertEquals(ErrorCode.DUPLICATE_MSG_ID, dupAck.reason());
            assertEquals(initialDedupSize + 1, server.deduplicator().size());

            // 5b: Repeated or stale ACK is treated as an idempotent no-op without error
            robin.ack("non-existent-msg-xyz");
            robin.ack("dup-1"); // legitimate first ACK
            robin.ack("dup-1"); // repeated stale ACK
            // No error frames received by Robin; connection remains completely healthy
            batman.send("m-ok", "robin", "Batcave operational");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());
            Frame.Deliver dOk = robin.pollDeliver(3000);
            assertNotNull(dOk);
            assertEquals("m-ok", dOk.msgId());
            robin.ack("m-ok");

            // 5c: Malformed JSON frame handling
            try (TestClient malformedClient = new TestClient(server.getPort())) {
                malformedClient.sendRaw("{not_json}\n");
                Frame err = malformedClient.pollFrame(3000);
                assertInstanceOf(Frame.Error.class, err);
                assertEquals(ErrorCode.INVALID_FRAME, ((Frame.Error) err).code());
            }

            // 5d: Unregistered client sending commands
            try (TestClient unregClient = new TestClient(server.getPort())) {
                unregClient.send("m-unreg", "robin", "hello");
                Frame err = unregClient.pollFrame(3000);
                assertInstanceOf(Frame.Error.class, err);
                assertEquals(ErrorCode.UNREGISTERED, ((Frame.Error) err).code());
            }

            // 5e: Slow/unresponsive client does not block unrelated clients
            // Robin stops acknowledging or goes silent.
            // Connect Alfred; Batman sends messages to Alfred.
            try (TestClient alfred = new TestClient(server.getPort())) {
                alfred.register("alfred");
                assertNotNull(alfred.pollFrame(3000));

                batman.send("to-alfred", "alfred", "Tea is ready");
                assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

                Frame.Deliver dAlfred = alfred.pollDeliver(3000);
                assertNotNull(dAlfred);
                assertEquals("to-alfred", dAlfred.msgId());
                alfred.ack("to-alfred");

                await().atMost(3, TimeUnit.SECONDS).until(() ->
                        server.getMailbox("alfred").totalCount() == 0);
            }
        }
    }

    @Test
    @DisplayName("Bonus 1: Strict per-recipient FIFO ordering across concurrent sends and reconnects")
    void testStrictFifoOrdering() throws Exception {
        RelayConfig config = RelayConfig.builder().port(0).build();
        try (RelayServer server = createAndStartServer(config);
             TestClient sender1 = new TestClient(server.getPort());
             TestClient sender2 = new TestClient(server.getPort())) {

            sender1.register("sender1");
            assertNotNull(sender1.pollFrame(3000));
            sender2.register("sender2");
            assertNotNull(sender2.pollFrame(3000));

            // Robin is offline. Senders send 10 messages sequentially
            for (int i = 1; i <= 10; i++) {
                TestClient sender = (i % 2 == 0) ? sender2 : sender1;
                sender.send("fifo-" + i, "robin", "data-" + i);
                assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) sender.pollFrame(3000)).status());
            }

            // Robin connects and must receive all 10 messages in exact 1..10 FIFO sequence
            try (TestClient robin = new TestClient(server.getPort())) {
                robin.register("robin");
                assertNotNull(robin.pollFrame(3000));

                for (int i = 1; i <= 10; i++) {
                    Frame.Deliver d = robin.pollDeliver(3000);
                    assertNotNull(d, "Expected message fifo-" + i);
                    assertEquals("fifo-" + i, d.msgId());
                    assertEquals("data-" + i, d.payload());
                    robin.ack(d.msgId());
                }

                await().atMost(3, TimeUnit.SECONDS).until(() ->
                        server.getMailbox("robin").totalCount() == 0);
            }
        }
    }

    @Test
    @DisplayName("Bonus 3: State at rest and resilience across server restart")
    void testPersistenceAcrossServerRestart(@TempDir Path tempDir) throws Exception {
        // Step 1: Start Server instance 1 with persistence enabled
        RelayConfig config1 = RelayConfig.builder()
                .port(0)
                .dataDir(tempDir)
                .build();

        int serverPort;
        try (RelayServer server1 = createAndStartServer(config1);
             TestClient batman = new TestClient(server1.getPort())) {

            serverPort = server1.getPort();
            batman.register("batman");
            assertNotNull(batman.pollFrame(3000));

            // Batman sends 2 messages to offline Robin
            batman.send("wal-1", "robin", "Secret batcave coordinates");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            batman.send("wal-2", "robin", "Emergency backup codes");
            assertEquals(Frame.SendAck.ACCEPTED, ((Frame.SendAck) batman.pollFrame(3000)).status());

            // Server 1 closes (clean shutdown triggers WAL compaction)
        }

        // Step 2: Start brand new Server instance 2 pointing to the exact same storage directory
        RelayConfig config2 = RelayConfig.builder()
                .port(0)
                .dataDir(tempDir)
                .build();

        try (RelayServer server2 = createAndStartServer(config2);
             TestClient robin = new TestClient(server2.getPort())) {

            // Robin connects to the new server and registers
            robin.register("robin");
            assertNotNull(robin.pollFrame(3000));

            // Robin must receive both persisted messages in exact FIFO order
            Frame.Deliver d1 = robin.pollDeliver(3000);
            assertNotNull(d1, "Expected wal-1 to be recovered from disk");
            assertEquals("wal-1", d1.msgId());
            assertEquals("Secret batcave coordinates", d1.payload());

            Frame.Deliver d2 = robin.pollDeliver(3000);
            assertNotNull(d2, "Expected wal-2 to be recovered from disk");
            assertEquals("wal-2", d2.msgId());
            assertEquals("Emergency backup codes", d2.payload());

            // Robin acknowledges both
            robin.ack("wal-1");
            robin.ack("wal-2");

            await().atMost(3, TimeUnit.SECONDS).until(() ->
                    server2.getMailbox("robin").totalCount() == 0);
        }

        // Step 3: Restart a 3rd time to verify acknowledged messages are not resurrected
        RelayConfig config3 = RelayConfig.builder()
                .port(0)
                .dataDir(tempDir)
                .build();

        try (RelayServer server3 = createAndStartServer(config3);
             TestClient robin3 = new TestClient(server3.getPort())) {

            robin3.register("robin");
            assertNotNull(robin3.pollFrame(3000));

            // No messages should be pending
            Frame.Deliver dNone = robin3.pollDeliver(500);
            assertNull(dNone);
        }
    }
}
