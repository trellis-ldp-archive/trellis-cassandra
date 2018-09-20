package edu.si.trellis.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link InputStream} that wraps a {@link ByteBuffer} to avoid copying byte arrays.
 *
 */
class ByteBufferInputStream extends InputStream {

    private static final int ENDOFSTREAM = -1;

    private final ByteBuffer buffer;

    private int readLimit, readSinceMark;

    ByteBufferInputStream(ByteBuffer b) {
        this.buffer = (ByteBuffer) b.mark();
        this.readSinceMark = 0;
        this.readLimit = buffer.remaining();
    }

    @Override
    public long skip(long n) throws IOException {
        int toSkip = (int) Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + toSkip);
        return toSkip;
    }

    @Override
    public synchronized void mark(int limit) {
        this.readLimit = limit;
        buffer.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        if (readSinceMark > readLimit)
            throw new IOException("Cannot read past read limit set by previous call to mark(" + readLimit + ")!");
        buffer.reset();
        readSinceMark = 0;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public int read() {
        if (!buffer.hasRemaining()) return ENDOFSTREAM;
        readSinceMark++;
        return Byte.toUnsignedInt(buffer.get());
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        if (!buffer.hasRemaining()) return ENDOFSTREAM;
        int availableLength = Math.min(length, buffer.remaining());
        buffer.get(bytes, offset, availableLength);
        readSinceMark += availableLength;
        return availableLength;
    }
}
