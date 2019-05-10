package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableReadConsistency;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to retrieve a list of the Mementos of a resource.
 */
public class Mementos extends ResourceQuery {

    @Inject
    public Mementos(Session session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT modified FROM " + MEMENTO_MUTABLE_TABLENAME + " WHERE identifier = :identifier",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the resource the Mementos of which are to be cataloged
     * @return A {@link ResultSet} with the modified-dates of any Mementos for this resource. There will be at least one
     *         (the most recent one).
     */
    public CompletableFuture<ResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
