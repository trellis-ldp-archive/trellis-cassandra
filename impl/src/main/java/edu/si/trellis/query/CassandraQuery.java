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

    /**
     * A Cassandra session for use with this query.
     */
    protected final Session session;

    /**
     * 
     */
    /**
     * Worker threads that read and write from and to Cassandra. Reading and writing are thereby uncoupled from threads
     * calling into this class.
     */
    protected final Executor writeWorkers = newCachedThreadPool(), readWorkers = newCachedThreadPool();

    private final PreparedStatement preparedStatement;

    /**
     * @return the {@link PreparedStatement} that underlies this query
     */
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

    /**
     * @param statement the CQL statement to execute
     * @return when and whether the statement completed
     */
    protected CompletableFuture<Void> executeWrite(BoundStatement statement) {
        String queryString = statement.preparedStatement().getQueryString();
        log.debug("Executing CQL write: {}", queryString);
        return translate(session.executeAsync(statement), writeWorkers)
                        .thenAccept(r -> log.debug("Executed CQL write: {}", queryString));
    }

    /**
     * @param statement the CQL statement to execute
     * @return the results of that statement
     */
    protected CompletableFuture<ResultSet> executeRead(Statement statement) {
        return translate(session.executeAsync(statement), readWorkers);
    }

    /**
     * @param statement the CQL statement to execute
     * @return the results of that statement
     */
    protected ResultSet executeSyncRead(Statement statement) {
        return session.execute(statement);
    }

    private <T> CompletableFuture<T> translate(ListenableFuture<T> future, Executor workers) {
        CompletableFuture<T> result = new CompletableFuture<>();
        future.addListener(() -> {
            try {
                result.complete(future.get()); // future::get will not block; see ListenableFuture#addListener
            } catch (InterruptedException e) {
                result.completeExceptionally(e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error(e.getLocalizedMessage(), e);
                result.completeExceptionally(e.getCause());
            }
        }, workers);
        return result;
    }
}
