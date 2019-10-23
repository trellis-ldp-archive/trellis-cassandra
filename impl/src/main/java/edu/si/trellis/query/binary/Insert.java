package edu.si.trellis.query.binary;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import edu.si.trellis.BinaryWriteConsistency;

import java.io.InputStream;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Insert binary data into a table.
 */
public class Insert extends BinaryQuery implements Executor {

    @Inject
    public Insert(CqlSession session, @BinaryWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + BINARY_TABLENAME + " (identifier, chunkSize, chunkIndex, chunk) VALUES "
                        + "(:identifier, :chunkSize, :chunkIndex, :chunk)", consistency);
    }

    /**
     * @param id the {@link IRI} of this binary
     * @param chunkSize size of chunk to use for this binary
     * @param chunkIndex which chunk this is
     * @param chunk the bytes of this chunk
     * @return whether and when it has been inserted
     */
    public CompletionStage<Void> execute(IRI id, int chunkSize, int chunkIndex, InputStream chunk) {
        BoundStatement boundStatement = preparedStatement().bind().set("identifier", id, IRI.class)
                        .setInt("chunkSize", chunkSize).setInt("chunkIndex", chunkIndex)
                        .set("chunk", chunk, InputStream.class);
        return executeWrite(boundStatement);
    }

    @Override
    public void execute(Runnable command) {
        writeWorkers.execute(command);
    }
}
