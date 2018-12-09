package edu.si.trellis.cassandra;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Metadata.builder;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.api.TrellisUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.vocabulary.LDP.BasicContainer;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.RDFSource;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.*;
import org.trellisldp.api.Metadata;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService extends CassandraService implements ResourceService, MementoService {

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    static final String MUTABLE_TABLENAME = "mutabledata";

    static final String IMMUTABLE_TABLENAME = "immutabledata";

    static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    private static final String GET_QUERY = "SELECT * FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? AND "
                    + "createdSeconds <= ? LIMIT 1 ALLOW FILTERING;";

    private static final String DELETE_QUERY = "DELETE FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? ";

    private static final String IMMUTABLE_INSERT_QUERY = "INSERT INTO " + IMMUTABLE_TABLENAME
                    + " (identifier, quads, created) VALUES (?,?,?)";

    private static final String MUTABLE_INSERT_QUERY = "INSERT INTO " + MUTABLE_TABLENAME
                    + " (interactionModel, size, mimeType, createdSeconds, container, quads, modified, binaryIdentifier, created, identifier)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?)";

    private static final String TOUCH_QUERY = "UPDATE " + MUTABLE_TABLENAME
                    + " SET modified=? WHERE created=? AND identifier=?";

    private static final String MEMENTOS_QUERY = "SELECT modified FROM " + MUTABLE_TABLENAME + " WHERE identifier = ?";

    private PreparedStatement getStatement, immutableInsertStatement, deleteStatement, mutableInsertStatement,
                    touchStatement, mementosStatement;

    private final ResourceQueryContext resourceQueries;

    /**
     * Constructor.
     * 
     * @param session a Cassandra {@link Session} for use by this service for its lifetime
     * @param readCons the read-consistency to use
     * @param writeCons the write-consistency to use
     */
    @Inject
    public CassandraResourceService(final Session session, @RdfReadConsistency ConsistencyLevel readCons,
                    @RdfWriteConsistency ConsistencyLevel writeCons) {
        super(session, readCons, writeCons);
        this.resourceQueries = new ResourceQueryContext(session, readConsistency());
    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeQueriesAndRoot() {
        log.debug("Preparing retrieval query: {}", GET_QUERY);
        this.getStatement = session().prepare(GET_QUERY).setConsistencyLevel(readConsistency());
        log.debug("Preparing deletion query: {}", DELETE_QUERY);
        this.deleteStatement = session().prepare(DELETE_QUERY).setConsistencyLevel(writeConsistency());
        log.debug("Preparing immmutable data insert query: {}", IMMUTABLE_INSERT_QUERY);
        this.immutableInsertStatement = session().prepare(IMMUTABLE_INSERT_QUERY)
                        .setConsistencyLevel(writeConsistency());
        log.debug("Preparing mutable data insert statement: {}", MUTABLE_INSERT_QUERY);
        this.mutableInsertStatement = session().prepare(MUTABLE_INSERT_QUERY).setConsistencyLevel(writeConsistency());
        log.debug("Preparing touch data update statement: {}", TOUCH_QUERY);
        this.touchStatement = session().prepare(TOUCH_QUERY).setConsistencyLevel(writeConsistency());
        log.debug("Preparing Mementos data retrieval statement: {}", MEMENTOS_QUERY);
        this.mementosStatement = session().prepare(MEMENTOS_QUERY).setConsistencyLevel(readConsistency());

        IRI rootIri = TrellisUtils.getInstance().createIRI(TRELLIS_DATA_PREFIX);
        try {
            if (get(rootIri).get().equals(MISSING_RESOURCE)) {
                Metadata rootResource = builder(rootIri).interactionModel(BasicContainer).build();
                create(rootResource, null).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    private Function<? super ResultSet, ? extends Resource> buildResource(IRI id) {
        return rows -> {
            final Row metadata = rows.one();
            boolean wasFound = metadata != null;
            log.debug("Resource {} was {}found", id, wasFound ? "" : "not ");
            if (!wasFound) return MISSING_RESOURCE;

            log.trace("Computing metadata for resource {}", id);
            IRI ixnModel = metadata.get("interactionModel", IRI.class);
            log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
            boolean hasAcl = metadata.getBool("hasAcl");
            log.debug("Found hasAcl = {} for resource {}", hasAcl, id);
            IRI binaryId = metadata.get("binaryIdentifier", IRI.class);
            log.debug("Found binaryIdentifier = {} for resource {}", binaryId, id);
            String mimeType = metadata.getString("mimetype");
            log.debug("Found mimeType = {} for resource {}", mimeType, id);
            long size = metadata.getLong("size");
            log.debug("Found size = {} for resource {}", size, id);
            IRI container = metadata.get("container", IRI.class);
            log.debug("Found container = {} for resource {}", container, id);
            Instant modified = metadata.get("modified", Instant.class);
            log.debug("Found modified = {} for resource {}", modified, id);
            UUID created = metadata.getUUID("created");
            log.debug("Found created = {} for resource {}", created, id);
            return new CassandraResource(id, ixnModel, hasAcl, binaryId, mimeType, size, container, modified, created,
                            resourceQueries);
        };
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        return get(id, theFuture());
    }

    private static Instant theFuture() {
        return now().plus(1, DAYS);
    }

    @Override
    public CompletableFuture<Resource> get(final IRI id, Instant time) {
        Statement boundStatement = getStatement.bind(id, time).setConsistencyLevel(readConsistency());
        log.debug("Executing CQL statement: {} with identifier: {}", getStatement.getQueryString(), id);
        return translate(session().executeAsync(boundStatement)).thenApply(buildResource(id));
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletableFuture<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        return executeAndDone(immutableInsertStatement.bind(id, dataset, now()));
    }

    @Override
    public CompletableFuture<Void> create(Metadata meta, Dataset data) {
        log.debug("Creating {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletableFuture<Void> replace(Metadata meta, Dataset data) {
        log.debug("Replacing {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletableFuture<Void> delete(Metadata meta) {
        log.debug("Deleting {}", meta.getIdentifier());
        return executeAndDone(deleteStatement.bind(meta.getIdentifier()));
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletableFuture<Void> touch(IRI id) {
        return get(id).thenApply(CassandraResource.class::cast).thenApply(CassandraResource::getCreated)
                        .thenCompose(created -> executeAndDone(touchStatement.bind(now(), created, id)));
    }

    @Override
    public CompletableFuture<Void> put(Resource resource) {
        // NOOP see our data model
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SortedSet<Instant>> mementos(IRI id) {
        return execute(mementosStatement.bind(id))
                        .thenApply(results -> stream(results::spliterator, NONNULL + DISTINCT, false)
                                        .map(getFieldAs("modified", Instant.class))
                                        .map(time -> time.truncatedTo(SECONDS)).collect(toCollection(TreeSet::new)));
    }

    private CompletableFuture<Void> write(Metadata meta, Dataset data) {
        IRI id = meta.getIdentifier();
        IRI ixnModel = meta.getInteractionModel();
        Instant now = now();

        Optional<BinaryMetadata> binary = meta.getBinary();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        Long size = binary.flatMap(BinaryMetadata::getSize).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);
        IRI container = meta.getContainer().orElse(null);

        return executeAndDone(mutableInsertStatement.bind(ixnModel, size, mimeType, now.truncatedTo(SECONDS), container,
                        data, now, binaryIdentifier, UUIDs.timeBased(), id));
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }

    static class ResourceQueryContext {

        private static final String mutableQuadStreamQuery = "SELECT quads FROM " + MUTABLE_TABLENAME
                        + "  WHERE identifier = ? AND createdSeconds <= ? LIMIT 1 ALLOW FILTERING;";

        private static final String immutableQuadStreamQuery = "SELECT quads FROM " + IMMUTABLE_TABLENAME
                        + "  WHERE identifier = ? ;";

        private static final String basicContainmentQuery = "SELECT identifier AS contained FROM "
                        + BASIC_CONTAINMENT_TABLENAME + " WHERE container = ? ;";

        private final Session session;

        private PreparedStatement mutableQuadStreamStatement, immutableQuadStreamStatement, basicContainmentStatement;

        ResourceQueryContext(Session session, ConsistencyLevel consistency) {
            this.session = session;
            log.trace("Preparing " + getClass().getSimpleName() + " queries.");
            this.mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery)
                            .setConsistencyLevel(consistency);
            this.immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery)
                            .setConsistencyLevel(consistency);
            this.basicContainmentStatement = session.prepare(basicContainmentQuery)
                            .setConsistencyLevel(consistency);
            log.trace("Prepared " + getClass().getSimpleName() + " queries.");
        }

        PreparedStatement basicContainmentStatement() {
            return basicContainmentStatement;
        }

        PreparedStatement mutableQuadStreamStatement() {
            return mutableQuadStreamStatement;
        }

        PreparedStatement immutableQuadStreamStatement() {
            return immutableQuadStreamStatement;
        }

        Session session() {
            return session;
        }
    }
}
