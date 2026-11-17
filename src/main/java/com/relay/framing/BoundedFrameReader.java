package com.relay.framing;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads UTF-8 newline-delimited lines from an InputStream, strictly enforcing
 * a maximum byte limit per frame to guard against OutOfMemory attacks.
 */
public class BoundedFrameReader implements AutoCloseable {

    private final InputStream in;
    private final int maxFrameBytes;
    private final ByteArrayOutputStream buffer;

    public BoundedFrameReader(InputStream in, int maxFrameBytes) {
        if (maxFrameBytes <= 0) {
            throw new IllegalArgumentException("maxFrameBytes must be > 0: " + maxFrameBytes);
        }
        // Wrap with BufferedInputStream for fast memory-buffered reads without per-byte syscall overhead
        this.in = (in instanceof BufferedInputStream) ? in : new BufferedInputStream(in, 8192);
        this.maxFrameBytes = maxFrameBytes;
        this.buffer = new ByteArrayOutputStream(Math.min(maxFrameBytes, 1024));
    }

    /**
     * Reads the next newline-delimited frame string.
     *
     * @return the frame string (excluding the delimiter), or null if the end of stream was reached cleanly.
     * @throws PayloadTooLargeException if the frame exceeds maxFrameBytes before encountering a newline delimiter.
     * @throws IOException on socket or stream I/O errors.
     */
    public String readNextFrame() throws IOException {
        buffer.reset();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                return extractFrame();
            }

            buffer.write(b);
            if (buffer.size() > maxFrameBytes) {
                throw new PayloadTooLargeException(
                        "Frame size exceeded maximum allowed limit of " + maxFrameBytes + " bytes"
                );
            }
        }

        // EOF reached cleanly
        if (buffer.size() == 0) {
            return null;
        }

        // EOF reached with unterminated frame
        return extractFrame();
    }

    private String extractFrame() {
        byte[] bytes = buffer.toByteArray();
        int len = bytes.length;
        if (len > 0 && bytes[len - 1] == '\r') {
            len--; // strip optional carriage return
        }
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
