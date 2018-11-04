package edu.si.trellis.cassandra;

import static com.google.common.collect.Streams.concat;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.TrellisUtils.toQuad;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.PreferMembership;
import static org.trellisldp.vocabulary.LDP.getSuperclassOf;

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
import org.trellisldp.api.Binary;
import org.trellisldp.api.Resource;
import org.trellisldp.api.TrellisUtils;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private final IRI identifier;

    private final IRI binaryIdentifier, container, interactionModel;

    private final String mimeType;

    private final long size;

    private final boolean hasAcl, isContainer;

    private final Instant modified, creation;

    private final ResourceQueries queries;

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, long size,
                    IRI container, Instant modified, Instant creation, ResourceQueries queries) {
        this.identifier = id;
        this.interactionModel = ixnModel;
        this.isContainer = getInteractionModel() == null ? false
                        : Container.equals(getInteractionModel())
                                        || Container.equals(getSuperclassOf(getInteractionModel()));
        this.hasAcl = hasAcl;
        this.binaryIdentifier = binaryIdentifier;
        this.mimeType = mimeType;
        this.size = size;
        this.container = container;
        log.trace("Resource is {}a container.", !isContainer ? "not " : "");
        this.modified = modified;
        this.creation = creation;
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
     * @return the creation for this resource
     */
    public Instant getCreation() {
        return creation;
    }

    @Override
    public Boolean hasAcl() {
        return hasAcl;
    }

    @Override
    public Optional<Binary> getBinary() {
        return Optional.ofNullable(isBinary() ? new Binary(binaryIdentifier, modified, mimeType, size) : null);
    }

    private boolean isBinary() {
        return LDP.NonRDFSource.equals(getInteractionModel());
    }

    @Override
    public Stream<? extends Quad> stream() {
        log.trace("Retrieving quad stream for resource {}", getIdentifier());
        Stream<Quad> mutableQuads = quadStreamFromQuery(queries.mutableQuadStreamStatement().bind(getIdentifier()));
        Stream<Quad> immutableQuads = quadStreamFromQuery(queries.immutableQuadStreamStatement().bind(getIdentifier()));
        Stream<Quad> containmentQuadsInContainment = isContainer
                        ? basicContainmentTriples().map(toQuad(PreferContainment))
                        : empty();
        Stream<Quad> containmentQuadsInMembership = isContainer
                        ? basicContainmentTriples().map(toQuad(PreferMembership))
                        : empty();
        return concat(mutableQuads, containmentQuadsInContainment, containmentQuadsInMembership, immutableQuads);
    }

    private Stream<Triple> basicContainmentTriples() {
        RDF rdfFactory = TrellisUtils.getInstance();
        final Spliterator<Row> rows = queries.session()
                        .execute(queries.basicContainmentStatement().bind(getIdentifier()))
                        .spliterator();
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
