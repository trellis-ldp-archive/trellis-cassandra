package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableWriteConsistency;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;

/**
 * A query to insert mutable data about a resource into Cassandra.
 */
public class MutableInsert extends ResourceQuery {

    @Inject
    public MutableInsert(Session session, @MutableWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + MUTABLE_TABLENAME
                        + " (interactionModel, mimeType, container, hasAcl, quads, modified, binaryIdentifier, created, identifier)"
                        + " VALUES (?,?,?,?,?,?,?,?,?);", consistency);
    }

    /**
     * @param ixnModel an {@link IRI} for the interaction model for this resource
     * @param mimeType if this resource has a binary, the mimeType therefor
     * @param container if this resource has a container, the {@link IRI} therefor
     * @param data RDF for this resource
     * @param modified the time at which this resource was last modified
     * @param binaryIdentifier if this resource has a binary, the identifier therefor
     * @param creation a time-based (version 1) UUID for the moment this resource is created
     * @param id an {@link IRI} that identifies this resource
     * @return whether and when it has been inserted
     */
    public CompletableFuture<Void> execute(IRI ixnModel, String mimeType, IRI container,
                    boolean hasAcl, Dataset data, Instant modified, IRI binaryIdentifier, UUID creation, IRI id) {
        return executeWrite(preparedStatement().bind(ixnModel, mimeType, container, hasAcl, data, modified,
                        binaryIdentifier, creation, id));
    }
}
