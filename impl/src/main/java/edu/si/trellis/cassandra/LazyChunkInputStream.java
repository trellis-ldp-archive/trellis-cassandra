package edu.si.trellis.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.InputStream;

import org.trellisldp.api.RuntimeTrellisException;

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

    private final BoundStatement query;

    public LazyChunkInputStream(Session session, BoundStatement query) {
        this.session = session;
        this.query = query;
    }

    @Override
    protected void initialize() {
        Row row = session.execute(query).one();
        if (row == null) throw new RuntimeTrellisException("Missing binary chunk!");
        wrap(row.get("chunk", InputStream.class));
    }
}
