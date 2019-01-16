package edu.si.trellis.query.binary;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.BinaryReadConsistency;

import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Reads all bytes from a binary to an {@link InputStream}.
 *
 */
public class Read extends BinaryReadQuery {

    @Inject
    public Read(Session session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkIndex FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    /**
     * @param id the {@link IRI} for a binary
     * @return An {@link InputStream} of bytes as requested. The {@code skip} method of this {@code InputStream} is
     *         guaranteed to skip as many bytes as asked.
     * 
     * @see BinaryReadQuery#retrieve(IRI, com.datastax.driver.core.Statement)
     */
    public InputStream execute(IRI id) {
        BoundStatement bound = preparedStatement().bind(id);
        return retrieve(id, bound);
    }
}
