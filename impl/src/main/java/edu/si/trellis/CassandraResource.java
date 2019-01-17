package edu.si.trellis;

import static com.datastax.driver.core.utils.UUIDs.unixTimestamp;
import static java.util.stream.Stream.concat;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;
import static org.trellisldp.api.TrellisUtils.toQuad;
import static org.trellisldp.vocabulary.LDP.*;

import com.datastax.driver.core.Row;

import edu.si.trellis.query.rdf.BasicContainment;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MutableRetrieve;

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

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private final IRI identifier, container, interactionModel;

    private final boolean hasAcl, isContainer;

    private final Instant modified;

    private final UUID created;

    private final BinaryMetadata binary;

    private final ImmutableRetrieve immutable;

    private final MutableRetrieve mutable;

    private final BasicContainment bcontainment;

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, IRI container,
                    Instant modified, UUID created, ImmutableRetrieve immutable, MutableRetrieve mutable,
                    BasicContainment bcontainment) {
        this.identifier = id;
        this.interactionModel = ixnModel;
        this.isContainer = Container.equals(getInteractionModel())
                        || Container.equals(getSuperclassOf(getInteractionModel()));
        this.hasAcl = hasAcl;
        this.container = container;
        log.trace("Resource is {}a container.", !isContainer ? "not " : "");
        this.modified = modified;
        boolean isBinary = NonRDFSource.equals(getInteractionModel());
        this.binary = isBinary ? builder(binaryIdentifier).mimeType(mimeType).build() : null;
        log.trace("Resource is {}a NonRDFSource.", !isBinary ? "not " : "");
        this.created = created;
        this.mutable = mutable;
        this.immutable = immutable;
        this.bcontainment = bcontainment;
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
    public boolean hasAcl() {
        return hasAcl;
    }

    @Override
    public Optional<BinaryMetadata> getBinaryMetadata() {
        return Optional.ofNullable(binary);
    }

    @Override
    public Stream<Quad> stream() {
        log.trace("Retrieving quad stream for resource {}", getIdentifier());
        long createdMs = unixTimestamp(getCreated());
        Stream<Quad> mutableQuads = mutable.execute(getIdentifier(), createdMs);
        Stream<Quad> immutableQuads = immutable.execute(getIdentifier());
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
        final Spliterator<Row> rows = bcontainment.execute(getIdentifier()).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(r -> r.get("contained", IRI.class));
        return contained.map(cont -> rdfFactory.createTriple(getIdentifier(), contains, cont))
                        .peek(t -> log.trace("Built containment triple: {}", t));
    }
}
