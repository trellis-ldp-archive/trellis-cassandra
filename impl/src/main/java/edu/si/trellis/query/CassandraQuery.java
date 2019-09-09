package edu.si.trellis.query;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.RuntimeTrellisException;

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

    private final PreparedStatement preparedStatement;

    /**
     * @return the {@link PreparedStatement} that underlies this query
     */
    protected PreparedStatement preparedStatement() {
        return preparedStatement;
    }

    /**
     * @param session a {@link CqlSession} to the Cassandra cluster
     * @param queryString the CQL string for this query
     * @param consistency the {@link ConsistencyLevel} to use for executions of this query
     */
    public CassandraQuery(CqlSession session, String queryString, ConsistencyLevel consistency) {
        this.session = session;
        final SimpleStatement sStmt = new SimpleStatementBuilder(queryString).setConsistencyLevel(consistency).build();
        this.preparedStatement = session.prepare(sStmt);
    }

    /**
     * @param statement the CQL statement to execute
     * @return when and whether the statement completed
     */
    protected CompletionStage<Void> executeWrite(BoundStatement statement) {
        return session.executeAsync(statement).thenAccept(r -> {});
    }

    /**
     * @param statement the CQL statement to execute
     * @return the results of that statement
     */
    protected CompletionStage<AsyncResultSet> executeRead(BoundStatement statement) {
        return session.executeAsync(statement).exceptionally(e->{
            log.error("Failed request to Cassandra with ", e);
            throw new RuntimeTrellisException(e);
        });
    }
}
