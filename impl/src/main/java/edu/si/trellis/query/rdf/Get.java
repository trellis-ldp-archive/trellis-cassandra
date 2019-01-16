package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfReadConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

public class Get extends CassandraQuery {

    @Inject
    public Get(Session session, @RdfReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT * FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? AND "
                        + "createdSeconds <= ? LIMIT 1 ALLOW FILTERING;", consistency);
    }

    public CompletableFuture<ResultSet> execute(IRI id, Instant time) {
        return executeRead(preparedStatement().bind(id, time));
    }
}