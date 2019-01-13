package edu.si.trellis.query.binary;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.BinaryWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query that deletes a binary.
 *
 */
public class Delete extends CassandraQuery {

    @Inject
    public Delete(Session session, @BinaryWriteConsistency ConsistencyLevel consistency) {
        super(session, "DELETE FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    /**
     * @param id an {@link IRI} for a binary to delete
     * @return whether and when it has been deleted
     */
    public CompletableFuture<Void> execute(IRI id) {
        return executeWrite(preparedStatement().bind(id));
    }
}
