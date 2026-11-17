package com.relay.framing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(5)
class BoundedFrameReaderTest {

    @Test
    @DisplayName("Reads single newline-delimited line successfully")
    void readsSingleLine() throws IOException {
        String input = "{\"type\":\"REGISTER\",\"clientId\":\"batman\"}\n";
        try (BoundedFrameReader reader = new BoundedFrameReader(
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), 1024)) {
            String line = reader.readNextFrame();
            assertEquals("{\"type\":\"REGISTER\",\"clientId\":\"batman\"}", line);
            assertNull(reader.readNextFrame());
        }
    }

    @Test
    @DisplayName("Handles CRLF line endings cleanly")
    void handlesCrlf() throws IOException {
        String input = "line1\r\nline2\r\n";
        try (BoundedFrameReader reader = new BoundedFrameReader(
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), 1024)) {
            assertEquals("line1", reader.readNextFrame());
            assertEquals("line2", reader.readNextFrame());
            assertNull(reader.readNextFrame());
        }
    }

    @Test
    @DisplayName("Throws PayloadTooLargeException when frame exceeds byte bound")
    void throwsOnOversizedFrame() throws IOException {
        // Limit is 10 bytes, send 15 bytes without newline
        String oversized = "0123456789ABCDEF\n";
        try (BoundedFrameReader reader = new BoundedFrameReader(
                new ByteArrayInputStream(oversized.getBytes(StandardCharsets.UTF_8)), 10)) {
            assertThrows(PayloadTooLargeException.class, reader::readNextFrame);
        }
    }

    @Test
    @DisplayName("Returns null on clean empty EOF")
    void returnsNullOnEmptyEof() throws IOException {
        try (BoundedFrameReader reader = new BoundedFrameReader(
                new ByteArrayInputStream(new byte[0]), 1024)) {
            assertNull(reader.readNextFrame());
        }
    }
}
