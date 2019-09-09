package edu.si.trellis.query.binary;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.BinaryWriteConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query that deletes a binary.
 *
 */
public class Delete extends BinaryQuery {

    @Inject
    public Delete(CqlSession session, @BinaryWriteConsistency ConsistencyLevel consistency) {
        super(session, "DELETE FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    /**
     * @param id an {@link IRI} for a binary to delete
     * @return whether and when it has been deleted
     */
    public CompletionStage<Void> execute(IRI id) {
        return executeWrite(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
