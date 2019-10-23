package edu.si.trellis.query;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.util.concurrent.CompletionStage;
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
    protected final CqlSession session;

    /**
     * 
     */
    /**
     * Worker threads that read and write from and to Cassandra. Reading and writing are thereby uncoupled from threads
     * calling into this class.
     */
    protected final Executor writeWorkers = newCachedThreadPool(), readWorkers = newCachedThreadPool(),
                    readBinaryWorkers = newCachedThreadPool();

    private final PreparedStatement preparedStatement;

    private final ConsistencyLevel consistency;

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
    public CassandraQuery(CqlSession session, String queryString, ConsistencyLevel consistency) {
        this.session = session;
        this.preparedStatement = session.prepare(queryString);
        this.consistency = consistency;
    }

    /**
     * @param statement the CQL statement to execute
     * @return when and whether the statement completed
     */
    protected CompletionStage<Void> executeWrite(BoundStatement statement) {
        String queryString = statement.getPreparedStatement().getQuery();
        log.debug("Executing CQL write: {}", queryString);
        BoundStatement consistentStatement = statement.setConsistencyLevel(consistency);
        return session.executeAsync(consistentStatement)
                        .thenAccept(r -> log.debug("Executed CQL write: {}", queryString));
    }

    /**
     * @param statement the CQL statement to execute
     * @return the results of that statement
     */
    protected CompletionStage<AsyncResultSet> executeRead(BoundStatement statement) {
        return session.executeAsync(statement);
    }

    /**
     * @param statement the CQL statement to execute
     * @return the results of that statement
     */
    protected ResultSet executeSyncRead(BoundStatement statement) {
        return session.execute(statement);
    }
}
