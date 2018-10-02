package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Session;
import com.google.common.base.Throwables;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.*;
import org.slf4j.Logger;
import org.trellisldp.api.*;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private static final RDF factory = RDFUtils.getInstance();

    public static final String mutableQuadStreamQuery = "SELECT quads FROM trellis." + Mutable.tableName
                    + "  WHERE identifier = ? LIMIT 1 ;";

    public static final String immutableQuadStreamQuery = "SELECT quads FROM trellis." + Immutable.tableName
                    + "  WHERE identifier = ? ;";

    public static final String metadataQuery = "SELECT identifier, interactionModel, hasAcl, binaryIdentifier, "
                    + "mimeType, size, container, WRITETIME(interactionModel) AS modified FROM trellis."
                    + Mutable.tableName + " WHERE identifier = ? LIMIT 1 ;";

    private BoundStatement mutableQuadStreamStatement, immutableQuadStreamStatement, metadataStatement;

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
        mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery).bind(getIdentifier());
        immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery).bind(getIdentifier());
        metadataStatement = session.prepare(metadataQuery).bind(getIdentifier());
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
        log.debug("Looking for container for resource {}", getIdentifier());
        Stream<Quad> containmentQuad = getContainer().map(this::containmentQuad).map(Stream::of).orElse(empty());
        return concat(concat(mutableQuads, immutableQuads), containmentQuad);
    }

    private Quad containmentQuad(IRI container) {
        log.debug("Found container for resource {}", getIdentifier());
        return factory.createQuad(PreferServerManaged, getIdentifier(), LDP.contains, container);
    }

    private Stream<Quad> quadStreamFromQuery(final BoundStatement boundStatement) {
        final Spliterator<Row> rows = session.execute(boundStatement).spliterator();
        return StreamSupport.stream(rows, false).flatMap(row -> row.get("quads", Dataset.class).stream());
    }
}
