package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;

public class MutableInsert extends CassandraQuery {
    
    @Inject
    public MutableInsert(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + MUTABLE_TABLENAME
                        + " (interactionModel, mimeType, createdSeconds, container, quads, modified, binaryIdentifier, created, identifier)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)", consistency);
    }

    public CompletableFuture<Void> execute(IRI ixnModel, String mimeType, Instant createdSeconds, IRI container,
                    Dataset data, Instant modified, IRI binaryIdentifier, UUID creation, IRI id) {
        return executeWrite(preparedStatement().bind(ixnModel, mimeType, createdSeconds, container, data, modified,
                        binaryIdentifier, creation, id));
    }
}
