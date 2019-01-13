package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

public class Touch extends CassandraQuery {

    @Inject
    public Touch(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "UPDATE " + MUTABLE_TABLENAME + " SET modified=? WHERE created=? AND identifier=?", consistency);
    }

    public CompletableFuture<Void> execute(Instant modified, UUID created, IRI id) {
        return executeWrite(preparedStatement().bind(modified, created, id));
    }
}
