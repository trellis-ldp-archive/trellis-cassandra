package edu.si.trellis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
                    + " (identifier, quads, modified) VALUES (?,?,?)";
    private PreparedStatement getStatement, immutableInsertStatement, deleteStatement;

    private final ResourceQueries resourceQueries;

    /**
     * Constructor.
     * 
     * @param session a Cassandra {@link Session} for use by this service for its lifetime
     */
    @Inject
    public CassandraResourceService(final Session session) {
        super(session);
        this.resourceQueries = new ResourceQueries(session);
    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeQueriesAndRoot() {
        log.debug("Preparing retrieval query: {}", GET_QUERY);
        this.getStatement = session().prepare(GET_QUERY);
        log.debug("Preparing deletion query: {}", DELETE_QUERY);
        this.deleteStatement = session().prepare(DELETE_QUERY);
        log.debug("Preparing immmutable data insert query: {}", IMMUTABLE_INSERT_QUERY);
        this.immutableInsertStatement = session().prepare(IMMUTABLE_INSERT_QUERY);

        IRI rootIri = TrellisUtils.getInstance().createIRI(TRELLIS_DATA_PREFIX);
        Metadata rootResource = builder(rootIri).interactionModel(BasicContainer).build();
        try {
            create(rootResource, null).get();
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
            Long size = metadata.getLong("size");
            log.debug("Found size = {} for resource {}", size, id);
            IRI container = metadata.get("container", IRI.class);
            log.debug("Found container = {} for resource {}", container, id);
            Instant modified = metadata.get("modified", Instant.class);
            log.debug("Found modified = {} for resource {}", modified, id);
            Instant created = metadata.get("created", Instant.class);
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
        BoundStatement boundStatement = getStatement.bind(id, time);
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
        return execute(immutableInsertStatement.bind(id, dataset, now()));
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
        return execute(deleteStatement.bind(meta.getIdentifier()));
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletableFuture<Void> touch(IRI id) {
        return get(id).thenApply(CassandraResource.class::cast).thenApply(CassandraResource::getCreated)
                        .thenCompose(created -> execute(update(MUTABLE_TABLENAME).where(eq("identifier", id))
                                        .and(eq("created", created)).with(set("modified", now()))));
    }

    @Override
    public CompletableFuture<Void> put(Resource resource) {
        // NOOP see our data model
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SortedSet<Instant>> mementos(IRI id) {
        Where query = select("modified").from(MUTABLE_TABLENAME).where(eq("identifier", id));
        return read(query).thenApply(results -> stream(results::spliterator, NONNULL + DISTINCT, false)
                        .map(getFieldAs("modified", Instant.class)).map(time -> time.truncatedTo(SECONDS))
                        .collect(toCollection(TreeSet::new)));
    }

    private static <T> Function<Row, T> getFieldAs(String k, Class<T> klass) {
        return row -> row.get(k, klass);
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

        //@formatter:off
        return execute(insertInto(MUTABLE_TABLENAME)
                        .value("interactionModel", ixnModel)
                        .value("size", size)
                        .value("mimeType", mimeType)
                        .value("createdSeconds", now.truncatedTo(SECONDS))
                        .value("container", container)
                        .value("quads", data)
                        .value("modified", now)
                        .value("binaryIdentifier", binaryIdentifier)
                        .value("created", now)
                        .value("identifier", id));
        //@formatter:on
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }

    static class ResourceQueries {

        private static final String mutableQuadStreamQuery = "SELECT quads FROM " + MUTABLE_TABLENAME
                        + "  WHERE identifier = ? AND createdSeconds <= ? LIMIT 1 ALLOW FILTERING;";

        private static final String immutableQuadStreamQuery = "SELECT quads FROM " + IMMUTABLE_TABLENAME
                        + "  WHERE identifier = ? ;";

        private static final String basicContainmentQuery = "SELECT identifier AS contained FROM "
                        + BASIC_CONTAINMENT_TABLENAME + " WHERE container = ? ;";

        private Session session;

        private PreparedStatement mutableQuadStreamStatement, immutableQuadStreamStatement, basicContainmentStatement;

        @Inject
        ResourceQueries(Session session) {
            this.session = session;
            prepareQueries();
        }

        void prepareQueries() {
            log.trace("Preparing " + getClass().getSimpleName() + " queries.");
            this.mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery);
            this.immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery);
            this.basicContainmentStatement = session.prepare(basicContainmentQuery);
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
