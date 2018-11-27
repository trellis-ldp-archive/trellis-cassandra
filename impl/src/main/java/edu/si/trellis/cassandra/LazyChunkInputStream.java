package edu.si.trellis.cassandra;

import java.io.IOException;
import java.io.InputStream;

public class LazyChunkInputStream extends InputStream {

    private InputStream chunk;

    @Override
    public int read() throws IOException {
        checkInitialized();
        return chunk.read();
    }

    private void checkInitialized() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
        if (chunk != null) chunk.close();
    }

}
