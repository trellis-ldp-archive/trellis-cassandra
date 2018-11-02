package edu.si.trellis.cassandra;

import static com.google.common.collect.Streams.concat;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.TrellisUtils.toQuad;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.PreferMembership;
import static org.trellisldp.vocabulary.LDP.getSuperclassOf;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;
import org.trellisldp.api.TrellisUtils;
import org.trellisldp.api.Resource;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private static final String mutableQuadStreamQuery = "SELECT quads FROM " + Mutable.tableName
                    + "  WHERE identifier = ? LIMIT 1 ;";

    private static final String immutableQuadStreamQuery = "SELECT quads FROM " + Immutable.tableName
                    + "  WHERE identifier = ? ;";

    private static final String basicContainmentQuery = "SELECT contained FROM basiccontainment WHERE identifier = ? ;";

    private BoundStatement mutableQuadStreamStatement, immutableQuadStreamStatement, basicContainmentStatement;

    private Session session;

    private final IRI identifier;

    private final IRI binaryIdentifier, container, interactionModel;

    private final String mimeType;

    private final long size;

    private final boolean hasAcl, isContainer;

    private final Instant modified, timestamp;

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, long size,
                    IRI container, Instant modified, Instant timestamp, Session session) {
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
        this.modified = modified;
        this.timestamp = timestamp;
        this.session = session;

        synchronized (this) {
            if (mutableQuadStreamStatement == null) prepareQueries();
        }
    }

    private synchronized void prepareQueries() {
        log.trace("Preparing " + getClass().getSimpleName() + " queries.");
        mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery).bind(getIdentifier());
        immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery).bind(getIdentifier());
        basicContainmentStatement = session.prepare(basicContainmentQuery).bind(getIdentifier());
        log.trace("Prepared " + getClass().getSimpleName() + " queries.");
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
     * @return the timestamp for this resource
     */
    public Instant getTimestamp() {
        return timestamp;
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
        Stream<Quad> mutableQuads = quadStreamFromQuery(mutableQuadStreamStatement);
        Stream<Quad> immutableQuads = quadStreamFromQuery(immutableQuadStreamStatement);
        Stream<Quad> containmentQuadsInContainment = isContainer
                        ? basicContainmentTriples().map(toQuad(PreferContainment))
                        : empty();
        Stream<Quad> containmentQuadsInMembership = isContainer
                        ? basicContainmentTriples().map(toQuad(PreferMembership))
                        : empty();
        return concat(mutableQuads, containmentQuadsInContainment, containmentQuadsInMembership, immutableQuads);
    }

    private Stream<Triple> basicContainmentTriples() {
        final Spliterator<Row> rows = session.execute(basicContainmentStatement).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(get("contained", IRI.class));
        return contained.map(cont -> TrellisUtils.getInstance().createTriple(getIdentifier(), LDP.contains, cont))
                        .peek(t -> log.trace("Built containment triple: {}", t));
    }

    private Stream<Quad> quadStreamFromQuery(final BoundStatement boundStatement) {
        final Spliterator<Row> rows = session.execute(boundStatement).spliterator();
        Stream<Dataset> datasets = StreamSupport.stream(rows, false).map(get("quads", Dataset.class));
        return datasets.flatMap(Dataset::stream);
    }

    private static <T> Function<Row, T> get(String k, Class<T> klass) {
        return row -> row.get(k, klass);
    }
}
