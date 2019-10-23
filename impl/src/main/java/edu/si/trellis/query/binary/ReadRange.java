package edu.si.trellis.query.binary;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import edu.si.trellis.BinaryReadConsistency;

import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Reads a range of bytes from a binary to an {@link InputStream}
 *
 */
public class ReadRange extends BinaryReadQuery {

    @Inject
    public ReadRange(CqlSession session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkIndex FROM " + BINARY_TABLENAME
                        + " WHERE identifier = :identifier and chunkIndex >= :start and chunkIndex <= :end;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of a binary to read
     * @param first which byte to begin reading on
     * @param last which byte to end reading on
     * @return An {@link InputStream} of bytes as requested. The {@code skip} method of this {@code InputStream} is
     *         guaranteed to skip as many bytes as asked.
     * 
     * @see BinaryReadQuery#retrieve(IRI, com.datastax.driver.core.Statement)
     */
    public InputStream execute(IRI id, int first, int last) {
        BoundStatement bound = preparedStatement().bind()
                        .set("identifier", id, IRI.class)
                        .setInt("start", first)
                        .setInt("end", last);
        return retrieve(id, bound);
    }
}
