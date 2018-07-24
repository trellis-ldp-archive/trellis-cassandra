package edu.si.trellis.cassandra;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link InputStream} that wraps a {@link ByteBuffer} to avoid copying byte arrays.
 *
 */
class ByteBufferInputStream extends InputStream {

    private static final int ENDOFSTREAM = -1;

    private final ByteBuffer buffer;

    ByteBufferInputStream(ByteBuffer b) {
        this.buffer = b;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public int read() {
        if (!buffer.hasRemaining()) return ENDOFSTREAM;
        return Byte.toUnsignedInt(buffer.get()); 
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        if (!buffer.hasRemaining()) return ENDOFSTREAM;
        int availableLength = Math.min(length, buffer.remaining());
        buffer.get(bytes, offset, availableLength);
        return availableLength;
    }
}
