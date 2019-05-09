package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfReadConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to retrieve a list of the Mementos of a resource.
 */
public class Mementos extends CassandraQuery {

    @Inject
    public Mementos(Session session, @RdfReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT mementomodified FROM " + MEMENTO_MUTABLE_TABLENAME + " WHERE identifier = :identifier",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the resource the Mementos of which are to be cataloged
     * @return A {@link ResultSet} with the modified-dates of any Mementos for this resource. There will be at least one
     *         (the most recent one).
     */
    public CompletableFuture<ResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind(id));
    }
}
