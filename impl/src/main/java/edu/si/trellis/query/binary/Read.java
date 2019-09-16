package edu.si.trellis.query.binary;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

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
    public Read(CqlSession session, @BinaryReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT chunkIndex, chunk FROM " + BINARY_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    /**
     * @param id the {@link IRI} for a binary
     * @return A future for an {@link InputStream} of bytes as requested. The {@code skip} method of this
     *         {@code InputStream} is guaranteed to skip as many bytes as asked.
     * 
     * @see BinaryReadQuery#retrieve(IRI, BoundStatement)
     */
    public InputStream execute(IRI id) {
        BoundStatement bound = preparedStatement().bind().set("identifier", id, IRI.class);
        return retrieve(bound);
    }
}
