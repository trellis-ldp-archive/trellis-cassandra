package edu.si.trellis.cassandra;

import static java.util.stream.Stream.concat;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;
import static org.trellisldp.api.TrellisUtils.toQuad;
import static org.trellisldp.vocabulary.LDP.*;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;

import edu.si.trellis.cassandra.CassandraResourceService.ResourceQueries;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.*;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.TrellisUtils;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private final IRI identifier, container, interactionModel;

    private final boolean hasAcl, isContainer;

    private final Instant modified, created;

    private final ResourceQueries queries;

    private final BinaryMetadata binary;

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, long size,
                    IRI container, Instant modified, Instant created, ResourceQueries queries) {
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
     * Unlike the value of {@link #getModified()}, this value is immutable after a resource is persisted.
     * 
     * @return the created date for this resource
     */
    public Instant getCreated() {
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
        BoundStatement mutableQuadStreamQuery = queries.mutableQuadStreamStatement().bind(getIdentifier(), getCreated());
        Stream<Quad> mutableQuads = quadStreamFromQuery(mutableQuadStreamQuery);
        Stream<Quad> immutableQuads = quadStreamFromQuery(queries.immutableQuadStreamStatement().bind(getIdentifier()));
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
        final Spliterator<Row> rows = queries.session()
                        .execute(queries.basicContainmentStatement().bind(getIdentifier())).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(getFieldAs("contained", IRI.class));
        return contained.map(cont -> rdfFactory.createTriple(getIdentifier(), LDP.contains, cont))
                        .peek(t -> log.trace("Built containment triple: {}", t));
    }

    private Stream<Quad> quadStreamFromQuery(final BoundStatement boundStatement) {
        final Spliterator<Row> rows = queries.session().execute(boundStatement).spliterator();
        Stream<Dataset> datasets = StreamSupport.stream(rows, false).map(getFieldAs("quads", Dataset.class));
        return datasets.flatMap(Dataset::stream);
    }

    private static <T> Function<Row, T> getFieldAs(String k, Class<T> klass) {
        return row -> row.get(k, klass);
    }
}
