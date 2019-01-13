package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

public class Delete extends CassandraQuery {

    @Inject
    public Delete(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "DELETE FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? ", consistency);
    }

    public CompletableFuture<Void> execute(IRI id) {
        return executeWrite(preparedStatement().bind(id));
    }
}
