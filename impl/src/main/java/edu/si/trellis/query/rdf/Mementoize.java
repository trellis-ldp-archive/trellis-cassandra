package edu.si.trellis.query.rdf;

import static java.time.temporal.ChronoUnit.SECONDS;

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
 * A query that records a version of a resource as a Memento.
 */
public class Mementoize extends CassandraQuery {

    @Inject
    public Mementoize(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "INSERT INTO " + MEMENTO_MUTABLE_TABLENAME
                        + " (interactionModel, mimeType, container, quads, modified, binaryIdentifier, "
                        + "created, identifier, mementomodified)" + " VALUES (?,?,?,?,?,?,?,?,?);", consistency);
    }

    /**
     * Store a Memento. Note that the value for {@code modified} is truncated to seconds because Memento requires HTTP
     * time management.
     * 
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
    public CompletableFuture<Void> execute(IRI ixnModel, String mimeType, IRI container, Dataset data, Instant modified,
                    IRI binaryIdentifier, UUID creation, IRI id) {
        final Instant mementoModified = modified.truncatedTo(SECONDS);
        return executeWrite(preparedStatement().bind(ixnModel, mimeType, container, data, modified, binaryIdentifier,
                        creation, id, mementoModified));
    }
}
