package edu.si.trellis;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

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

    private final CqlSession session;

    private final BoundStatement query;

    /**
     * @param session The Cassandra session to use
     * @param query the CQL query to use
     */
    public LazyChunkInputStream(CqlSession session, BoundStatement query) {
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
