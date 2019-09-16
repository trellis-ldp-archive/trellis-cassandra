package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.MutableWriteConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to delete a resource.
 */
public class Delete extends ResourceQuery {

    @Inject
    public Delete(CqlSession session, @MutableWriteConsistency ConsistencyLevel consistency) {
        super(session, "DELETE FROM " + MUTABLE_TABLENAME + " WHERE identifier = :identifier ;", consistency);
    }

    /**
     * @param id the {@link IRI} of the resource to delete
     * @return whether and when it has been deleted
     */
    public CompletionStage<Void> execute(IRI id) {
        return executeWrite(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
