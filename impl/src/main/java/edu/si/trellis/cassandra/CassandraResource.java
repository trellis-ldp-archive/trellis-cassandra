package edu.si.trellis.cassandra;

import static com.datastax.driver.core.utils.UUIDs.unixTimestamp;
import static java.util.stream.Stream.concat;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;
import static org.trellisldp.api.TrellisUtils.toQuad;
import static org.trellisldp.vocabulary.LDP.*;

import com.datastax.driver.core.Row;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.TrellisUtils;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private final IRI identifier, container, interactionModel;

    private final boolean hasAcl, isContainer;

    private final Instant modified;

    private final UUID created;

    private final ResourceQueryContext queries;

    private final BinaryMetadata binary;

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, long size,
                    IRI container, Instant modified, UUID created, ResourceQueryContext queries) {
        this.identifier = id;
        this.interactionModel = ixnModel;
        this.isContainer = Container.equals(getInteractionModel())
                        || Container.equals(getSuperclassOf(getInteractionModel()));
        this.hasAcl = hasAcl;
        this.container = container;
        log.trace("Resource is {}a container.", !isContainer ? "not " : "");
        this.modified = modified;
        boolean isBinary = NonRDFSource.equals(getInteractionModel());
        this.binary = isBinary ? builder(binaryIdentifier).mimeType(mimeType).size(size).build() : null;
        log.trace("Resource is {}a NonRDFSource.", !isBinary ? "not " : "");
        this.created = created;
        this.queries = queries;
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    /**
     * @return a container for this resource
     */
    @Override
    public Optional<IRI> getContainer() {
        return Optional.ofNullable(container);
    }

    @Override
    public IRI getInteractionModel() {
        return interactionModel;
    }

    @Override
    public Instant getModified() {
        return modified;
    }

    /**
     * Unlike the value of {@link #getModified()}, this value is immutable after a resource record is persisted. The
     * value of {@link #getModified()}, on the other hand, can change for containers if a child is added or removed, via
     * {@link ResourceService#touch}.
     * 
     * @return the created date for this resource
     */
    public UUID getCreated() {
        return created;
    }

    @Override
    public Boolean hasAcl() {
        return hasAcl;
    }

    @Override
    public Optional<BinaryMetadata> getBinaryMetadata() {
        return Optional.ofNullable(binary);
    }

    @Override
    public Stream<Quad> stream() {
        log.trace("Retrieving quad stream for resource {}", getIdentifier());
        Long createdMs = unixTimestamp(getCreated());
        Stream<Quad> mutableQuads = queries.mutableQuadStream(getIdentifier(), createdMs);
        Stream<Quad> immutableQuads = queries.immutableQuadStream(getIdentifier());
        Stream<Quad> quads = concat(mutableQuads, immutableQuads);
        if (isContainer) {
            Stream<Quad> containmentQuadsInContainment = basicContainmentTriples().map(toQuad(PreferContainment));
            Stream<Quad> containmentQuadsInMembership = basicContainmentTriples().map(toQuad(PreferMembership));
            quads = concat(quads, concat(containmentQuadsInContainment, containmentQuadsInMembership));
        }
        return quads;
    }

    private Stream<Triple> basicContainmentTriples() {
        RDF rdfFactory = TrellisUtils.getInstance();
        final Spliterator<Row> rows = queries.containment(getIdentifier()).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(r -> r.get("contained", IRI.class));
        return contained.map(cont -> rdfFactory.createTriple(getIdentifier(), LDP.contains, cont))
                        .peek(t -> log.trace("Built containment triple: {}", t));
    }
}
