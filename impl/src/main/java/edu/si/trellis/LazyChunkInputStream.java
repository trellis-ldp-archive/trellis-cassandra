package edu.si.trellis;

import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import java.io.InputStream;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

/**
 * An {@link InputStream} backed by a Cassandra query to retrieve one binary chunk.
 * <p>
 * Not thread-safe!
 * </p>
 * 
 * @see InputStreamCodec
 */
public class LazyChunkInputStream extends LazyFilterInputStream {

    private static final Logger log = getLogger(LazyChunkInputStream.class);

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
    protected CompletionStage<InputStream> initialize() {
        log.trace("Initializing…");
        CompletionStage<AsyncResultSet> executeAsync = session.executeAsync(query);
        log.trace("Retrieved bytes…");
        return executeAsync.thenApply(AsyncResultSet::one)
                        .thenApply(row -> requireNonNull(row, "Missing binary chunk!"))
                        .thenApply(row -> row.get("chunk", InputStream.class))
                        .thenApply(is -> {log.debug("Retrieved chunk");return is;});
    }
}
