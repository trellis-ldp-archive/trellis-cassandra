package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;

public class ImmutableInsert extends CassandraQuery {

    @Inject
    public ImmutableInsert(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + IMMUTABLE_TABLENAME + " (identifier, quads, created) VALUES (?,?,?)",
                        consistency);
    }

    public CompletableFuture<Void> execute(IRI id, Dataset data, Instant time) {
        return executeWrite(preparedStatement().bind(id, data, time));
    }
}
