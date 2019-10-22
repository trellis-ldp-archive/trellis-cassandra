package edu.si.trellis.query.binary;

import static java.util.Objects.requireNonNull;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.si.trellis.BinaryReadConsistency;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to retrieve the chunk size metadata for a binary.
 *
 */
public class GetChunkSize extends BinaryQuery {

    @Inject
    public GetChunkSize(Session session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkSize FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier LIMIT 1;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the binary to retrieve
     * @return a {@link Row} with the chunk size for this binary
     */
    public CompletableFuture<Row> execute(IRI id) {
        BoundStatement statement = preparedStatement().bind().set("identifier", id, IRI.class);
        return executeRead(statement, readBinaryWorkers)
                        .thenApply(ResultSet::one)
                        .thenApply(row -> requireNonNull(row,
                                        () -> "Binary not found under IRI: " + id.getIRIString() + " !"));
    }
}
