package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.MutableWriteConsistency;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query that adjusts the modified time of a resource.
 */
public class Touch extends ResourceQuery {

    @Inject
    public Touch(CqlSession session, @MutableWriteConsistency ConsistencyLevel consistency) {
        super(session, "UPDATE " + MUTABLE_TABLENAME + " SET modified = :modified WHERE identifier = :identifier",
                        consistency);
    }

    /**
     * @param modified the new modification time to record
     * @param id the {@link IRI} of the resource to modify
     * @return whether and when the modification succeeds
     */
    public CompletionStage<Void> execute(Instant modified, IRI id) {
        return executeWrite(preparedStatement().bind()
                        .set("modified", modified, Instant.class)
                        .set("identifier", id, IRI.class));
    }
}
