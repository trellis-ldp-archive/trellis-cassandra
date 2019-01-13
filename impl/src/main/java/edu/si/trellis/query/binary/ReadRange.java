package edu.si.trellis.query.binary;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

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
    public ReadRange(Session session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkIndex FROM " + BINARY_TABLENAME
                        + " WHERE identifier = :identifier and chunkIndex >= :start and chunkIndex <= :end;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of a binary to read
     * @param first which byte to begin reading on
     * @param last which byte to end reading on
     * @return an {@link InputStream} of bytes as requested
     */
    public InputStream execute(IRI id, int first, int last) {
        BoundStatement bound = preparedStatement().bind().set("identifier", id, IRI.class).setInt("start", first)
                        .setInt("end", last);
        return retrieve(id, bound);
    }
}
