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

public class InputStreamCodecTest {

    @Test
    public void emptyStringShouldParseWithNoBytes() throws IOException {
        try (InputStream testResult = inputStreamCodec.parse("")) {
            assertEquals(-1, testResult.read(), "Parsed InputStream should have no bytes!");
        }
    }

    @Test
    public void nullStringShouldParseAsNull() throws IOException {
        try (InputStream testResult = inputStreamCodec.parse(null)) {
            assertNull(testResult, "Parsed null InputStream should be null!");
        }
    }

    @Test
    public void nullByteBufferShouldDeserializeAsNull() throws IOException {
        try (InputStream testResult = inputStreamCodec.deserialize(null, null)) {
            assertNull(testResult, "Parsed null InputStream should be null!");
        }
    }

    @Test
    public void nullInputStreamShouldFormatAsNull() throws IOException {
        String testResult = inputStreamCodec.format(null);
        assertNull(testResult, "Parsed null InputStream should be null!");
    }

    @Test
    public void nullInputStreamShouldSerializeAsNull() throws IOException {
        ByteBuffer testResult = inputStreamCodec.serialize(null, null);
        assertNull(testResult, "Parsed null InputStream should be null!");
    }

    @Test
    public void emptyInputStreamShouldSerializeAsEmpty() throws IOException {
        ByteBuffer testResult = inputStreamCodec.serialize(new ByteArrayInputStream(new byte[] {}), null);
        assertFalse(testResult.hasRemaining(), "Parsed null InputStream should be null!");
    }

    @Test
    public void emptyByteBufferShouldParseAsEmpty() throws IOException {
        ByteBuffer testBuffer = ByteBuffer.wrap(new byte[] {});
        try (InputStream testResult = inputStreamCodec.deserialize(testBuffer, null)) {
            assertEquals(-1, testResult.read(), "Parsed null InputStream should be null!");
        }
    }

    @Test
    public void emptyStringShouldParseAsEmpty() throws IOException {
        try (InputStream testResult = inputStreamCodec.parse("")) {
            assertEquals(-1, testResult.read(), "Parsed null InputStream should be null!");
        }
    }
}
