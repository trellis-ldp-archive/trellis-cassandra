package edu.si.trellis.query;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.slf4j.Logger;

/**
 * A context for queries run against Cassandra. All requests to Cassandra should go through a subclass.
 *
 */
public abstract class CassandraQuery {

    private static final Logger log = getLogger(CassandraQuery.class);

    protected static final String MUTABLE_TABLENAME = "mutabledata";

    protected static final String IMMUTABLE_TABLENAME = "immutabledata";

    protected static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    protected static final String BINARY_TABLENAME = "binarydata";

    protected final Session session;

    protected final Executor writeWorkers = newCachedThreadPool(), readWorkers = newCachedThreadPool();

    private final PreparedStatement preparedStatement;

    protected PreparedStatement preparedStatement() {
        return preparedStatement;
    }

    /**
     * @param session a {@link Session} to the Cassandra cluster
     * @param queryString the CQL string for this query
     * @param consistency the {@link ConsistencyLevel} to use for executions of this query
     */
    public CassandraQuery(Session session, String queryString, ConsistencyLevel consistency) {
        this.session = session;
        this.preparedStatement = session.prepare(queryString).setConsistencyLevel(consistency);
    }

    protected CompletableFuture<Void> executeWrite(BoundStatement statement) {
        String queryString = statement.preparedStatement().getQueryString();
        log.debug("Executing CQL write: {}", queryString);
        return translate(session.executeAsync(statement), writeWorkers)
                        .thenAccept(r -> log.debug("Executed CQL write: {}", queryString));
    }

    protected CompletableFuture<ResultSet> executeRead(Statement statement) {
        return translate(session.executeAsync(statement), readWorkers);
    }

    protected ResultSet executeSyncRead(Statement statement) {
        return session.execute(statement);
    }

    protected <T> CompletableFuture<T> translate(ListenableFuture<T> future, Executor workers) {
        CompletableFuture<T> result = new CompletableFuture<>();
        future.addListener(() -> {
            try {
                result.complete(future.get()); // future::get will not block; see ListenableFuture#addListener
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                result.completeExceptionally(e.getCause());
            }
        }, workers);
        return result;
    }
}
