package edu.si.trellis.cassandra;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;

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

    protected final Executor workers = newCachedThreadPool();

    /**
     * @param session a {@link Session} to the Cassandra cluster
     */
    public QueryContext(Session session) {
        this.session = session;
    }

    protected CompletableFuture<Void> executeAndDone(Statement statement) {
        log.debug("Executing CQL statement: {}", statement);
        return execute(statement).thenAccept(r -> log.debug("Executed CQL statement: {}", statement));
    }

    protected CompletableFuture<ResultSet> execute(Statement statement) {
        return translate(session.executeAsync(statement));
    }

    protected ResultSet executeSync(Statement statement) {
        return session.execute(statement);
    }

    protected <T> CompletableFuture<T> translate(ListenableFuture<T> result) {
        return supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                // we don't know that persistence failed but we can't assume that it succeeded
                log.error("Error in persistence!", e.getCause());
                throw new CompletionException(e.getCause());
            }
        }, workers);
    }
}
