package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to delete a resource.
 */
public class Delete extends CassandraQuery {

    @Inject
    public Delete(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "DELETE FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? ", consistency);
    }

    /**
     * @param id the {@link IRI} of the resource to delete
     * @return whether and when it has been deleted
     */
    public CompletableFuture<Void> execute(IRI id) {
        return executeWrite(preparedStatement().bind(id));
    }
}
