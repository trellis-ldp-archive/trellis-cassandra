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

/**
 * A query to insert mutable data about a resource into Cassandra.
 */
public class MutableInsert extends CassandraQuery {

    @Inject
    public MutableInsert(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + MUTABLE_TABLENAME
                        + " (interactionModel, mimeType, createdSeconds, container, quads, modified, binaryIdentifier, created, identifier)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)", consistency);
    }

    /**
     * @param ixnModel an {@link IRI} for the interaction model for this resource
     * @param mimeType if this resource has a binary, the mimeType therefor
     * @param createdSeconds the time at which this resource was created truncated to seconds (used to support Memento)
     * @param container if this resource has a container, the {@link IRI} therefor
     * @param data RDF for this resource
     * @param modified the time at which this resource was last modified
     * @param binaryIdentifier if this resource has a binary, the identifier therefor
     * @param creation a time-based (version 1) UUID for the moment this resource is created
     * @param id an {@link IRI} that identifies this resource
     * @return whether and when it has been inserted
     */
    public CompletableFuture<Void> execute(IRI ixnModel, String mimeType, Instant createdSeconds, IRI container,
                    Dataset data, Instant modified, IRI binaryIdentifier, UUID creation, IRI id) {
        return executeWrite(preparedStatement().bind(ixnModel, mimeType, createdSeconds, container, data, modified,
                        binaryIdentifier, creation, id));
    }
}
