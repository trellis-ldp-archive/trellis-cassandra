package edu.si.trellis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.time.Instant.now;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Resource.SpecialResources.DELETED_RESOURCE;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.api.TrellisUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.vocabulary.LDP.BasicContainer;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.RDFSource;
import static org.trellisldp.vocabulary.Trellis.DeletedResource;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.rdf.api.*;
import org.slf4j.Logger;
import org.trellisldp.api.*;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService extends CassandraService implements ResourceService {

    @SuppressWarnings("boxing")
    private static final Long ZERO_LENGTH = 0L;

    private static final Dataset EMPTY = TrellisUtils.getInstance().createDataset();

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    private static final String GET_QUERY = "SELECT * FROM " + Mutable.tableName + " WHERE identifier = ? LIMIT 1;";
    private static final String IMMUTABLE_INSERT_QUERY = "INSERT INTO " + Immutable.tableName
                    + " (identifier, quads, modified) VALUES (?,?,?)";
    private PreparedStatement getStatement, immutableInsertStatement;

    /**
     * Constructor.
     *
     * @param session() a Cassandra {@link session()} for use by this service for its lifetime
     */
    @Inject
    public CassandraResourceService(final Provider<Session> session) {
        super(session);
    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeRoot() {
        log.info("Preparing retrieval query: {}", GET_QUERY);
        this.getStatement = session().prepare(GET_QUERY);
        log.info("Preparing immmutable data insert query: {}", IMMUTABLE_INSERT_QUERY);
        this.immutableInsertStatement = session().prepare(IMMUTABLE_INSERT_QUERY);
        RDF rdf = TrellisUtils.getInstance();
        IRI rootIri = rdf.createIRI(TRELLIS_DATA_PREFIX);
        try {
            create(rootIri, LDP.BasicContainer, rdf.createDataset(), null, null).get(3, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    private Function<? super ResultSet, Object[]> buildArray = rows -> {
        Row row = rows.one();
        int rowSize = row.getColumnDefinitions().size();
        Object[] array = new Object[rowSize];;
        for (int i = 0; i < rowSize; i++)
            array[++i] = row.getObject(i);
        return array;
    };

    private Function<? super ResultSet, ? extends Resource> buildResource(IRI id) {
        return rows -> {
            log.debug("Resource {} was {}found", id, rows.isExhausted() ? "not " : "");
            if (rows.isExhausted()) return MISSING_RESOURCE;
            final Row metadata = rows.one();
            log.trace("Computing metadata for resource {}", id);
            IRI ixnModel = metadata.get("interactionModel", IRI.class);
            log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
            if (DeletedResource.equals(ixnModel)) return DELETED_RESOURCE;

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
            Instant writestamp = metadata.get("writestamp", Instant.class);
            log.debug("Found writestamp = {} for resource {}", writestamp, id);
            return new CassandraResource(id, ixnModel, hasAcl, binaryId, mimeType, size, container, modified,
                            writestamp, session());
        };
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        BoundStatement boundStatement = getStatement.bind(id);
        log.debug("Executing CQL statement: {} with identifier: {}", getStatement.getQueryString(), id);
        ResultSetFuture result = session().executeAsync(boundStatement);

        return translate(result).thenApply(buildResource(id));
    }

    @Override
    public Optional<IRI> getContainer(final IRI id) {
        return resynchronize(get(id))
                        .flatMap(r -> r instanceof CassandraResource ? ((CassandraResource) r).getContainer()
                                        : ResourceService.super.getContainer(id));
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletableFuture<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        BoundStatement immutableDataInsert = immutableInsertStatement.bind(id, dataset, now());
        log.debug("using CQL query: {}", immutableDataInsert);
        return execute(immutableDataInsert);
    }

    @Override
    public CompletableFuture<Void> create(IRI id, IRI ixnModel, Dataset dataset, IRI container, Binary binary) {
        log.debug("Creating {} with interaction model {}", id, ixnModel);
        return write(id, ixnModel, container, dataset);
    }

    @Override
    public CompletableFuture<Void> replace(final IRI id, final IRI ixnModel, final Dataset dataset, final IRI container,
                    final Binary binary) {
        log.debug("Replacing {} with interaction model {}", id, ixnModel);
        return write(id, ixnModel, container, dataset);
    }

    @Override
    public CompletableFuture<Void> delete(final IRI id, final IRI ixnModel) {
        log.debug("Deleting {} with interaction model {}", id, ixnModel);
        // clean out basic containment index
        // first, this resource can no longer be contained
        CompletableFuture<Void> containerIndexDelete = get(id).thenApply(Resource::getContainer)
                        .thenCompose(maybeContainer -> maybeContainer
                                        .map(container -> execute(QueryBuilder.delete().from("basiccontainment")
                                                        .where(eq("identifier", container)).and(eq("contained", id))))
                                        .orElse(completedFuture(null)));
        // second, this resource can no longer contain other resources
        Where containedDeleteStatement = QueryBuilder.delete().from("basiccontainment").where(eq("identifier", id));
        log.debug("Using CQL: {}", containedDeleteStatement);
        CompletableFuture<Void> containedIndexDelete = execute(containedDeleteStatement);
        CompletableFuture<Void> selfDelete = write(id, DeletedResource, null, null);
        return allOf(selfDelete, containedIndexDelete, containerIndexDelete);
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletableFuture<Void> touch(IRI id) {
        return get(id).thenApply(res -> (res instanceof CassandraResource) ? (CassandraResource) res : null)
                        .thenApply(CassandraResource::getTimestamp)
                        .thenCompose(writestamp -> execute(update(Mutable.tableName).where(eq("identifier", id))
                                        .and(eq("writestamp", writestamp)).with(set("modified", now()))));
    }

    enum Mutability {
        Mutable("mutabledata"), Immutable("immutabledata");

        private Mutability(String tName) {
            this.tableName = tName;
        }

        public final String tableName;
    }

    private CompletableFuture<Void> write(final IRI id, final IRI ixnModel, final IRI container, final Dataset quads) {
        final Dataset dataset = quads == null ? EMPTY : quads;
        RegularStatement updateStatement = dataset.getGraph(PreferServerManaged).map(serverManaged -> {
            // if this has a binary/bitstream, develop up the extra metadata therefor
            if (NonRDFSource.equals(ixnModel)) {
                log.debug("Detected resource has NonRDFSource type: {}", ixnModel);
                IRI binaryIdentifier = serverManaged.stream(id, DC.hasPart, null).map(Triple::getObject)
                                .map(IRI.class::cast).findFirst().orElseThrow(() -> new RuntimeTrellisException(
                                                "Binary persisted with no bitstream IRI!"));
                Long size = serverManaged.stream(binaryIdentifier, DC.extent, null).map(Triple::getObject)
                                .map(Literal.class::cast).map(Literal::getLexicalForm).map(Long::parseLong).findFirst()
                                .orElse(ZERO_LENGTH);
                String mimeType = serverManaged.stream(binaryIdentifier, DC.format, null).map(Triple::getObject)
                                .map(Literal.class::cast).map(Literal::getLexicalForm).findFirst()
                                .orElse("application/octet-stream");
                log.debug("Persisting a NonRDFSource at {} with bitstream at {} of size {} and mimeType {}.", id,
                                binaryIdentifier, size, mimeType);
                return insertInto(Mutable.tableName).value("interactionModel", ixnModel).value("size", size)
                                .value("mimeType", mimeType).value("container", container)
                                .value("binaryIdentifier", binaryIdentifier).value("writestamp", now())
                                .value("modified", now()).value("identifier", id);
            }
            // or else it's not a binary
            log.debug("Resource is an RDF Source.");
            return null;
            // so just create RDFSource update statement
        }).orElse(insertInto(Mutable.tableName).value("interactionModel", ixnModel).value("quads", dataset)
                        .value("container", container).value("writestamp", now()).value("modified", now())
                        .value("identifier", id));
        // basic containment indexing
        if (container != null) {
            // the relationship between container and identifier is swapped
            Insert bcIndexInsert = insertInto("basiccontainment").value("identifier", container).value("contained", id);
            return allOf(execute(bcIndexInsert), execute(updateStatement));
        }
        return execute(updateStatement);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }
}
