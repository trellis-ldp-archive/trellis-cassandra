package edu.si.trellis.cassandra;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.slf4j.Logger;

public abstract class QueryContext {

    private static final Logger log = getLogger(QueryContext.class);

    protected static final String MUTABLE_TABLENAME = "mutabledata";

    protected static final String IMMUTABLE_TABLENAME = "immutabledata";

    protected static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    protected final Session session;

    protected final Executor workers = newCachedThreadPool();

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
