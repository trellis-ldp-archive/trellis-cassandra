package edu.si.trellis;

import java.io.InputStream;

/**
 * An {@link InputStream} that counts the bytes read from it and does not propagate {@link #close()}.
 *
 */
public class NoopCloseCountingInputStream extends org.apache.commons.io.input.CountingInputStream {

    /**
     * @param in the {@link InputStream} to wrap
     */
    public NoopCloseCountingInputStream(InputStream in) {
        super(in);
    }

    @Override
    public void close() { /* NO OP */ }
}