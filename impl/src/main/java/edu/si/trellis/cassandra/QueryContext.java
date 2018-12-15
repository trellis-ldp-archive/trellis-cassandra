package edu.si.trellis.cassandra;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;

/**
 * A context for queries run against Cassandra. All requests to Cassandra should go through a subclass.
 *
 */
abstract class QueryContext {

    private static final Logger log = getLogger(QueryContext.class);

    protected static final String MUTABLE_TABLENAME = "mutabledata";

    protected static final String IMMUTABLE_TABLENAME = "immutabledata";

    protected static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    protected static final String BINARY_TABLENAME = "binarydata";

    protected final Session session;

    protected final Executor writeWorkers = newCachedThreadPool(), readWorkers = newCachedThreadPool();

    /**
     * @param session a {@link Session} to the Cassandra cluster
     */
    public QueryContext(Session session) {
        this.session = session;
    }

    protected CompletableFuture<Void> executeWrite(Statement statement) {
        log.debug("Executing CQL write: {}", statement);
        return translate(session.executeAsync(statement), writeWorkers)
                        .thenAccept(r -> log.debug("Executed CQL write: {}", statement));
    }

    protected CompletableFuture<ResultSet> executeRead(Statement statement) {
        return translate(session.executeAsync(statement), readWorkers);
    }

    protected ResultSet executeSyncRead(Statement statement) {
        return session.execute(statement);
    }

    protected <T> CompletableFuture<T> translate(ListenableFuture<T> f, Executor workers) {
        CompletableFuture<T> result = new CompletableFuture<>();
        f.addListener(() -> result.complete(Futures.getUnchecked(f)), workers);
        return result;
    }
}
