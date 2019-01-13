package edu.si.trellis.query.binary;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.BinaryWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

public class Delete extends CassandraQuery {

    @Inject
    public Delete(Session session, @BinaryWriteConsistency ConsistencyLevel consistency) {
        super(session, "DELETE FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    public CompletableFuture<Void> execute(IRI id) {
        return executeWrite(preparedStatement().bind(id));
    }
}
