package com.relay.mailbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(5)
class ClientMailboxTest {

    static class MockDeliveryChannel implements DeliveryChannel {
        final List<MessageRecord> delivered = new ArrayList<>();
        boolean failDeliveries = false;
        boolean closed = false;

        @Override
        public boolean deliver(MessageRecord message) {
            if (failDeliveries || closed) {
                return false;
            }
            delivered.add(message);
            return true;
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public void closeQuietly() {
            closed = true;
        }
    }

    @Test
    @DisplayName("Enforces mailbox capacity limit strictly")
    void enforcesCapacity() {
        ClientMailbox mailbox = new ClientMailbox("robin", 2);
        assertEquals("robin", mailbox.clientId());
        assertEquals(2, mailbox.maxCapacity());

        SendResult r1 = mailbox.enqueue(new MessageRecord("m1", "batman", "robin", "p1"));
        assertTrue(r1.accepted());

        SendResult r2 = mailbox.enqueue(new MessageRecord("m2", "batman", "robin", "p2"));
        assertTrue(r2.accepted());

        // 3rd message should be rejected
        SendResult r3 = mailbox.enqueue(new MessageRecord("m3", "batman", "robin", "p3"));
        assertFalse(r3.accepted());
        assertEquals("MAILBOX_FULL", r3.reason());
        assertEquals(2, mailbox.totalCount());
    }

    @Test
    @DisplayName("Delivers messages immediately in FIFO order to attached channel")
    void deliversInFifoOrder() {
        ClientMailbox mailbox = new ClientMailbox("robin", 10);
        assertFalse(mailbox.hasActiveChannel());

        MockDeliveryChannel channel = new MockDeliveryChannel();
        mailbox.attach(channel);
        assertTrue(mailbox.hasActiveChannel());

        mailbox.enqueue(new MessageRecord("m1", "batman", "robin", "p1"));
        mailbox.enqueue(new MessageRecord("m2", "batman", "robin", "p2"));

        assertEquals(2, channel.delivered.size());
        assertEquals("m1", channel.delivered.get(0).msgId());
        assertEquals("m2", channel.delivered.get(1).msgId());
        assertEquals(0, mailbox.pendingCount());
        assertEquals(2, mailbox.inFlightCount());
    }

    @Test
    @DisplayName("Restores unacknowledged in-flight messages in FIFO order upon reconnect")
    void restoresUnackedMessagesOnReconnect() {
        ClientMailbox mailbox = new ClientMailbox("robin", 10);
        MockDeliveryChannel channel1 = new MockDeliveryChannel();
        mailbox.attach(channel1);
        assertTrue(mailbox.hasActiveChannel());

        mailbox.enqueue(new MessageRecord("m1", "batman", "robin", "p1"));
        mailbox.enqueue(new MessageRecord("m2", "batman", "robin", "p2"));

        // Only m1 is acknowledged
        assertTrue(mailbox.acknowledge("m1"));
        assertEquals(1, mailbox.inFlightCount()); // m2 is unacked

        // Client disconnects
        mailbox.detach(channel1);
        assertFalse(mailbox.hasActiveChannel());
        assertEquals(1, mailbox.pendingCount()); // m2 restored to pending
        assertEquals(0, mailbox.inFlightCount());

        // An offline message arrives while client is disconnected
        mailbox.enqueue(new MessageRecord("m3", "batman", "robin", "p3"));
        assertEquals(2, mailbox.pendingCount());

        // Reconnect with new channel
        MockDeliveryChannel channel2 = new MockDeliveryChannel();
        mailbox.attach(channel2);

        // channel2 must receive m2 first, then m3 (strict FIFO)
        assertEquals(2, channel2.delivered.size());
        assertEquals("m2", channel2.delivered.get(0).msgId());
        assertEquals("m3", channel2.delivered.get(1).msgId());
    }

    @Test
    @DisplayName("Stale or repeated ACK is handled as an idempotent no-op")
    void handlesStaleAckIdempotently() {
        ClientMailbox mailbox = new ClientMailbox("robin", 10);
        MockDeliveryChannel channel = new MockDeliveryChannel();
        mailbox.attach(channel);

        mailbox.enqueue(new MessageRecord("m1", "batman", "robin", "p1"));

        // First ACK succeeds
        assertTrue(mailbox.acknowledge("m1"));
        assertEquals(0, mailbox.totalCount());

        // Repeated ACK is a no-op returning false
        assertFalse(mailbox.acknowledge("m1"));

        // Non-existent ACK is a no-op returning false
        assertFalse(mailbox.acknowledge("non-existent"));
    }
}
