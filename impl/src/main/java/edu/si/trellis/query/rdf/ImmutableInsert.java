package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableWriteConsistency;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;

/**
 * A query to insert immutable data about a resource into Cassandra.
 */
public class ImmutableInsert extends ResourceQuery {

    @Inject
    public ImmutableInsert(Session session, @MutableWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + IMMUTABLE_TABLENAME + " (identifier, quads, created) VALUES (?,?,?)",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the resource, immutable data for which is to be inserted
     * @param data the RDF to be inserted
     * @param time the time at which this RDF is to be recorded as inserted
     * @return whether and when the insertion succeeds
     */
    public CompletableFuture<Void> execute(IRI id, Dataset data, Instant time) {
        return executeWrite(preparedStatement().bind(id, data, time));
    }
}
