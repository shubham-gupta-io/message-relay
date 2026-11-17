package com.relay.framing;

import com.relay.protocol.Frame;
import com.relay.protocol.JsonCodec;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe writer for sending newline-delimited UTF-8 frames over an OutputStream.
 * Uses an explicit ReentrantLock to prevent interleaved writes from concurrent threads.
 */
public class BoundedFrameWriter implements AutoCloseable {

    private final OutputStream out;
    private final ReentrantLock writeLock = new ReentrantLock();

    public BoundedFrameWriter(OutputStream out) {
        this.out = (out instanceof BufferedOutputStream) ? out : new BufferedOutputStream(out, 8192);
    }

    /**
     * Serializes and writes a Frame followed by a newline delimiter, then flushes.
     */
    public void writeFrame(Frame frame) throws IOException {
        String json = JsonCodec.serialize(frame);
        writeRawLine(json);
    }

    /**
     * Writes raw text followed by a newline delimiter, then flushes.
     */
    public void writeRawLine(String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        writeLock.lock();
        try {
            out.write(bytes);
            out.write('\n');
            out.flush();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            out.close();
        } finally {
            writeLock.unlock();
        }
    }
}
