package edu.si.trellis.query.binary;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.BinaryReadConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

public class Get extends CassandraQuery {

    @Inject
    public Get(Session session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkSize FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier LIMIT 1;",
                        consistency);
    }

    public CompletableFuture<ResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind(id));
    }
}
