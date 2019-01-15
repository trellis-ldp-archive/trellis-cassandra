package edu.si.trellis.query.binary;

import static java.util.Objects.requireNonNull;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.si.trellis.BinaryReadConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to retrieve the chunk size metadata for a binary.
 *
 */
public class Get extends CassandraQuery {

    @Inject
    public Get(Session session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkSize FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier LIMIT 1;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the binary to retrieve
     * @return a {@link Row} with the chunk size for this binary
     */
    public CompletableFuture<Row> execute(IRI id) {
        return executeRead(preparedStatement().bind(id)).thenApply(rows -> requireNonNull(rows.one(),
                        () -> "Binary not found under IRI: " + id.getIRIString() + " !"));
    }
}
