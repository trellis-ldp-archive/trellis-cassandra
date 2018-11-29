package edu.si.trellis.cassandra;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Like {@link FilterInputStream} but lazier; does not fill the slot for wrapped {@link InputStream} until an operation
 * is called.
 * <p>
 * Not thread-safe!
 * </p>
 * 
 * @see FilterInputStream
 */
public abstract class LazyFilterInputStream extends InputStream {

    private InputStream wrapped;

    private InputStream wrapped() {
        if (wrapped == null) initialize();
        return wrapped;
    }

    abstract protected void initialize();

    protected void wrap(InputStream w) {
        this.wrapped = w;
    }

    @Override
    public int read() throws IOException {
        return wrapped().read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return wrapped().read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return wrapped().read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return wrapped().skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapped().available();
    }

    @Override
    public void close() throws IOException {
        if (wrapped != null) wrapped.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        wrapped().mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        wrapped().reset();
    }

    @Override
    public boolean markSupported() {
        return wrapped().markSupported();
    }
}
