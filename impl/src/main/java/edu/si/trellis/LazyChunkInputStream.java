package edu.si.trellis;

import static java.util.Objects.requireNonNull;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import java.io.InputStream;

/**
 * An {@link InputStream} backed by a Cassandra query to retrieve one binary chunk.
 * <p>
 * Not thread-safe!
 * </p>
 * 
 * @see InputStreamCodec
 */
public class LazyChunkInputStream extends LazyFilterInputStream {

    private final Session session;

    private final Statement query;

    /**
     * @param session The Cassandra session to use
     * @param query the CQL query to use
     */
    public LazyChunkInputStream(Session session, Statement query) {
        this.session = session;
        this.query = query;
    }

    @Override
    protected InputStream initialize() {
        Row row = session.execute(query).one();
        requireNonNull(row, "Missing binary chunk!");
        return row.get("chunk", InputStream.class);
    }
}
