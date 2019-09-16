package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.MutableWriteConsistency;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;

/**
 * A query to insert immutable data about a resource into Cassandra.
 */
public class ImmutableInsert extends ResourceQuery {

    @Inject
    public ImmutableInsert(CqlSession session, @MutableWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + IMMUTABLE_TABLENAME + " (identifier, quads, created) VALUES (?,?,?)",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the resource, immutable data for which is to be inserted
     * @param data the RDF to be inserted
     * @param time the time at which this RDF is to be recorded as inserted
     * @return whether and when the insertion succeeds
     */
    public CompletionStage<Void> execute(IRI id, Dataset data, Instant time) {
        return executeWrite(preparedStatement().bind(id, data, time));
    }
}
