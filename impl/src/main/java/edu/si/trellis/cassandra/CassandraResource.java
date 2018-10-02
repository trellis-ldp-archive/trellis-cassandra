package edu.si.trellis.cassandra;

import static com.google.common.collect.Streams.concat;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.RDFUtils.toQuad;
import static org.trellisldp.vocabulary.LDP.*;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Session;
import com.google.common.base.Throwables;
import com.google.common.collect.Streams;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.*;
import org.slf4j.Logger;
import org.trellisldp.api.*;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private static final String mutableQuadStreamQuery = "SELECT quads FROM trellis." + Mutable.tableName
                    + "  WHERE identifier = ? LIMIT 1 ;";

    private static final String immutableQuadStreamQuery = "SELECT quads FROM trellis." + Immutable.tableName
                    + "  WHERE identifier = ? ;";

    private static final String metadataQuery = "SELECT identifier, interactionModel, hasAcl, binaryIdentifier, "
                    + "mimeType, size, container, WRITETIME(interactionModel) AS modified FROM trellis."
                    + Mutable.tableName + " WHERE identifier = ? LIMIT 1 ;";

    private static final String basicContainmentQuery = "SELECT contained FROM trellis.basiccontainment WHERE identifier = ? ;";

    private BoundStatement mutableQuadStreamStatement, immutableQuadStreamStatement, metadataStatement,
                    basicContainmentStatement;

    private Session session;

    private final IRI identifier;

    private volatile IRI binaryIdentifier, container, interactionModel;

    private volatile String mimeType;

    private volatile long size;

    private volatile Boolean hasAcl;

    private volatile Instant modified;

    public CassandraResource(final IRI identifier, final Session session) {
        this.identifier = requireNonNull(identifier);
        this.session = requireNonNull(session);
        prepareQueries();
    }

    private synchronized void prepareQueries() {
        log.trace("Preparing " + getClass().getSimpleName() + " queries.");
        this.mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery).bind(getIdentifier());
        this.immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery).bind(getIdentifier());
        this.metadataStatement = session.prepare(metadataQuery).bind(getIdentifier());
        this.basicContainmentStatement = session.prepare(basicContainmentQuery).bind(getIdentifier());
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
        computeMetadata();
        return Optional.ofNullable(container);
    }

    @Override
    public IRI getInteractionModel() {
        computeMetadata();
        return interactionModel;
    }

    @Override
    public Instant getModified() {
        computeMetadata();
        return modified;
    }

    @Override
    public Boolean hasAcl() {
        computeMetadata();
        return hasAcl;
    }

    private synchronized void computeMetadata() {
        log.trace("Determining whether to compute metadata for resource {}", getIdentifier());
        // use any piece of memoized state for this test because it all gets set together below
        Boolean result = hasAcl;
        if (result == null) {
            log.trace("Computing metadata for resource {}", getIdentifier());
            final Row metadata = fetchMetadata();
            hasAcl = metadata.getBool("hasAcl");
            log.debug("Found hasAcl = {} for resource {}", hasAcl, getIdentifier());
            modified = Instant.ofEpochMilli(metadata.get("modified", Long.class));
            log.debug("Found modified = {} for resource {}", modified, getIdentifier());
            interactionModel = metadata.get("interactionModel", IRI.class);
            log.debug("Found interactionModel = {} for resource {}", interactionModel, getIdentifier());
            binaryIdentifier = metadata.get("binaryIdentifier", IRI.class);
            log.debug("Found binaryIdentifier = {} for resource {}", binaryIdentifier, getIdentifier());
            mimeType = metadata.getString("mimetype");
            log.debug("Found mimeType = {} for resource {}", mimeType, getIdentifier());
            size = metadata.getLong("size");
            log.debug("Found size = {} for resource {}", size, getIdentifier());
            container = metadata.get("container", IRI.class);
            log.debug("Found container = {} for resource {}", container, getIdentifier());
        }
    }

    @Override
    public Optional<Binary> getBinary() {
        return Optional.ofNullable(isBinary() ? new Binary(binaryIdentifier, modified, mimeType, size) : null);
    }

    private boolean isBinary() {
        return LDP.NonRDFSource.equals(getInteractionModel());
    }

    private Row fetchMetadata() {
        log.trace("Fetching metadata for resource {}", getIdentifier());
        return session.execute(metadataStatement).one();
    }

    @Override
    public Stream<? extends Quad> stream() {
        log.trace("Retrieving quad stream for resource {}", getIdentifier());

        Stream<Quad> mutableQuads = quadStreamFromQuery(mutableQuadStreamStatement);
        Stream<Quad> immutableQuads = quadStreamFromQuery(immutableQuadStreamStatement);
        Stream<Quad> containmentQuadsInContainment = containmentTriples().map(toQuad(PreferContainment));
        Stream<Quad> containmentQuadsInMembership = containmentTriples().map(toQuad(PreferMembership));
        return concat(mutableQuads, containmentQuadsInContainment, containmentQuadsInMembership, immutableQuads);
    }

    private Stream<Triple> containmentTriples() {
        final Spliterator<Row> rows = session.execute(basicContainmentStatement).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(get("contained", IRI.class));
        return contained.map(o -> RDFUtils.getInstance().createTriple(getIdentifier(), LDP.contains, o));
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
