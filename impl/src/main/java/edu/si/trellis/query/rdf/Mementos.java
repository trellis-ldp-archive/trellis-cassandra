package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to retrieve a list of the Mementos of a resource.
 */
public class Mementos extends ResourceQuery {

    @Inject
    public Mementos(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT modified FROM " + MEMENTO_MUTABLE_TABLENAME + " WHERE identifier = :identifier",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the resource the Mementos of which are to be cataloged
     * @return A {@link AsyncResultSet} with the modified-dates of any Mementos for this resource. There will be at least one
     *         (the most recent one).
     */
    public CompletionStage<AsyncResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
