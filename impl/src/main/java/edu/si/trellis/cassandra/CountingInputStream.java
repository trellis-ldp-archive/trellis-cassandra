package edu.si.trellis.cassandra;

import java.io.InputStream;

/**
 * An {@link InputStream} that counts the bytes read from it and does not propagate {@link #close()}.
 *
 */
public class CountingInputStream extends org.apache.commons.io.input.CountingInputStream {

    public CountingInputStream(InputStream in) {
        super(in);
    }

    @Override
    public void close() { /* NO OP */ }
}