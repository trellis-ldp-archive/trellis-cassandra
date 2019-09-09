package edu.si.trellis;

import static edu.si.trellis.InputStreamCodec.inputStreamCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class InputStreamCodecTest {

    @Test
    void emptyStringShouldParseWithNoBytes() throws IOException {
        try (InputStream testResult = inputStreamCodec.parse("")) {
            assertEquals(-1, testResult.read(), "Parsed InputStream should have no bytes!");
        }
    }

    @Test
    void nullStringShouldParseAsNull() throws IOException {
        try (InputStream testResult = inputStreamCodec.parse(null)) {
            assertNull(testResult, "Parsed null InputStream should be null!");
        }
    }

    @Test
    void nullByteBufferShouldDeserializeAsNull() throws IOException {
        try (InputStream testResult = inputStreamCodec.decode(null, null)) {
            assertNull(testResult, "Parsed null InputStream should be null!");
        }
    }

    @Test
    void nullInputStreamShouldFormatAsNull() {
        String testResult = inputStreamCodec.format(null);
        assertNull(testResult, "Parsed null InputStream should be null!");
    }

    @Test
    void nullInputStreamShouldSerializeAsNull() {
        ByteBuffer testResult = inputStreamCodec.encode(null, null);
        assertNull(testResult, "Parsed null InputStream should be null!");
    }

    @Test
    void emptyInputStreamShouldSerializeAsEmpty() {
        ByteBuffer testResult = inputStreamCodec.encode(new ByteArrayInputStream(new byte[] {}), null);
        assertFalse(testResult.hasRemaining(), "Parsed null InputStream should be null!");
    }

    @Test
    void emptyByteBufferShouldParseAsEmpty() throws IOException {
        ByteBuffer testBuffer = ByteBuffer.wrap(new byte[] {});
        try (InputStream testResult = inputStreamCodec.decode(testBuffer, null)) {
            assertEquals(-1, testResult.read(), "Parsed null InputStream should be null!");
        }
    }

    @Test
    void emptyStringShouldParseAsEmpty() throws IOException {
        try (InputStream testResult = inputStreamCodec.parse("")) {
            assertEquals(-1, testResult.read(), "Parsed null InputStream should be null!");
        }
    }
}
