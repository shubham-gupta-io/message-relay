package com.relay.server;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe, bounded deduplicator for inbound messages based on (senderId, msgId).
 */
public class MessageDeduplicator {

    private final Map<String, Boolean> lruMap;
    private final int maxEntries;
    private final ReentrantLock lock = new ReentrantLock();

    public MessageDeduplicator(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0: " + maxEntries);
        }
        this.maxEntries = maxEntries;
        // Access-order LinkedHashMap: automatically evicts the oldest entry when exceeding maxEntries
        this.lruMap = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > MessageDeduplicator.this.maxEntries;
            }
        };
    }

    public MessageDeduplicator() {
        this(50_000);
    }

    public int maxEntries() {
        return maxEntries;
    }

    public boolean checkAndRegister(String senderId, String msgId) {
        // Null byte separator prevents collisions between senderId and msgId (e.g. "a" + "bc" vs "ab" + "c")
        String key = senderId + "\u0000" + msgId;
        lock.lock();
        try {
            if (lruMap.containsKey(key)) {
                return false;
            }
            lruMap.put(key, Boolean.TRUE);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return lruMap.size();
        } finally {
            lock.unlock();
        }
    }
}
