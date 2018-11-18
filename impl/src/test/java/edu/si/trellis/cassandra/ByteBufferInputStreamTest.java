package edu.si.trellis.cassandra;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public class ByteBufferInputStreamTest {

    private final byte[] testByteArray = new byte[] { 1, 2, 3, 4, 3, 2, 1 };
    private final ByteBuffer testData = ByteBuffer.wrap(testByteArray);

    private ByteBuffer testData() {
        return testData.asReadOnlyBuffer();
    }

    @Test
    public void cantResetBeyondLimit() throws IOException {
        ByteBufferInputStream stream = new ByteBufferInputStream(testData());
        stream.mark(3);
        stream.read(new byte[8]);
        assertThrows(IOException.class, stream::reset);
    }

    @Test
    public void availableWorks() throws IOException {
        ByteBufferInputStream stream = new ByteBufferInputStream(testData());
        assertEquals(7, stream.available());
    }

    @Test
    public void supportsMark() {
        ByteBufferInputStream stream = new ByteBufferInputStream(testData());
        assertTrue(stream.markSupported());
    }

    @Test
    public void noMarkMeansResetTo0() throws IOException {
        ByteBufferInputStream stream = new ByteBufferInputStream(testData());
        byte[] answer = new byte[testByteArray.length];
        stream.read(new byte[3]);
        stream.reset();
        stream.read(answer);
        assertArrayEquals(testByteArray, answer);
    }

    @Test
    public void noBytesMeansNoBytes() throws IOException {
        ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.allocate(0));
        assertEquals(-1, stream.read());
        assertEquals(-1, stream.read(new byte[10]));
    }

    @Test
    public void skipWorks() throws IOException {
        ByteBufferInputStream stream = new ByteBufferInputStream(testData());
        byte[] answer = new byte[testByteArray.length];
        stream.skip(3);
        assertEquals(4, stream.read(answer));
    }

    @Test
    public void readWorks() throws IOException {
        ByteBufferInputStream stream = new ByteBufferInputStream(testData());
        for (int i : testByteArray) assertEquals(i, stream.read());
    }
}
