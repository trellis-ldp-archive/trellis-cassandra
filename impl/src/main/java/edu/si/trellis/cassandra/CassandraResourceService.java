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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.RDFUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.api.Resource.SpecialResources.DELETED_RESOURCE;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
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
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

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
public class CassandraResourceService implements ResourceService {

    @SuppressWarnings("boxing")
    private static final Long ZERO_LENGTH = 0L;

    private static final Dataset EMPTY = RDFUtils.getInstance().createDataset();

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    private final Session session;

    private static final String GET_QUERY = "SELECT * FROM trellis." + Mutable.tableName
                    + " WHERE identifier = ?;";
    private final PreparedStatement getStatement;

    private static final String IMMUTABLE_INSERT_QUERY = "INSERT INTO trellis." + Immutable.tableName
                    + " (identifier,quads) VALUES (?,?)";
    private final PreparedStatement immutableInsertStatement;

    /**
     * Same-thread execution. TODO use a pool?
     */
    private final Executor executor = Runnable::run;

    /**
     * Constructor.
     *
     * @param session a Cassandra {@link Session} for use by this service for its lifetime
     */
    @Inject
    public CassandraResourceService(final Session session) {
        this.session = session;
        log.info("Preparing retrieval query: {}", GET_QUERY);
        getStatement = session.prepare(GET_QUERY);
        log.info("Preparing immmutable data insert query: {}", IMMUTABLE_INSERT_QUERY);
        immutableInsertStatement = session.prepare(IMMUTABLE_INSERT_QUERY);
    }

    /**
     * Build a root container.
     */
    @PostConstruct
    public void initializeRoot() {
        RDF rdf = RDFUtils.getInstance();
        IRI rootIri = rdf.createIRI(TRELLIS_DATA_PREFIX);
        try {
            create(rootIri, LDP.BasicContainer, rdf.createDataset(), null, null).get(3, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        BoundStatement boundStatement = getStatement.bind(id);
        log.debug("Executing CQL statement: {} with identifier: {}", getStatement.getQueryString(), id);
        ResultSetFuture result = session.executeAsync(boundStatement);
        return translate(result).<Resource>thenApply(rows -> {
            log.debug("Resource {} was {}found", id, rows.isExhausted() ? "not " : "");
            if (rows.isExhausted()) return MISSING_RESOURCE;
            final Row metadata = rows.one();
            log.trace("Computing metadata for resource {}", id);
            IRI ixnModel = metadata.get("interactionModel", IRI.class);
            log.debug("Found interactionModel = {} for resource {}", ixnModel, id); 
            if (DeletedResource.equals(ixnModel)) return DELETED_RESOURCE;
            
            boolean hasAcl = metadata.getBool("hasAcl");
            log.debug("Found hasAcl = {} for resource {}", hasAcl, id);
            IRI binaryIdentifier = metadata.get("binaryIdentifier", IRI.class);
            log.debug("Found binaryIdentifier = {} for resource {}", binaryIdentifier, id);
            String mimeType = metadata.getString("mimetype");
            log.debug("Found mimeType = {} for resource {}", mimeType, id);
            long size = metadata.getLong("size");
            log.debug("Found size = {} for resource {}", size, id);
            IRI container = metadata.get("container", IRI.class);
            log.debug("Found container = {} for resource {}", container, id);
            Instant modified = metadata.get("modified", Instant.class);
            log.debug("Found modified = {} for resource {}", modified, id);
            
            return new CassandraResource(id, ixnModel, hasAcl, binaryIdentifier, mimeType, size, container, modified,
                            session);
        });
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
        BoundStatement immutableDataInsert = immutableInsertStatement.bind(id, dataset);
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
                                        .map(container -> execute(QueryBuilder.delete()
                                                        .from("trellis", "basiccontainment")
                                                        .where(eq("identifier", container)).and(eq("contained", id))))
                                        .orElse(completedFuture(null)));
        // second, this resource can no longer contain other resources
        Where containedDeleteStatement = QueryBuilder.delete().from("trellis", "basiccontainment")
                        .where(eq("identifier", id));
        log.debug("Using CQL: {}", containedDeleteStatement);
        CompletableFuture<Void> containedIndexDelete = execute(containedDeleteStatement);
        CompletableFuture<Void> containerUpdate = get(id).thenApply(Resource::getContainer)
                        .thenCompose(maybeContainer -> maybeContainer
                                        .map(container -> execute(update("trellis", Mutable.tableName)
                                                        .with(set("modified", now()))
                                                        .where(eq("identifier", container))))
                                        .orElse(completedFuture(null)));
        CompletableFuture<Void> selfDelete = write(id, DeletedResource, null, null);
        return allOf(selfDelete, containedIndexDelete, containerIndexDelete, containerUpdate);
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
            // if this has a binary/bitstream, pick up the extra metadata therefor
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
                return update("trellis", Mutable.tableName).with(set("interactionModel", ixnModel))
                                .and(set("size", size)).and(set("mimeType", mimeType)).and(set("container", container))
                                .and(set("binaryIdentifier", binaryIdentifier))
                                .and(set("modified", now())).where(eq("identifier", id));
            }
            // or else it's not a binary
            log.debug("Resource is an RDF Source.");
            return null;
            // so just create RDFSource update statement
        }).orElse(update("trellis", Mutable.tableName).with(set("interactionModel", ixnModel))
                        .and(set("quads", dataset)).and(set("container", container))
                        .and(set("modified", now())).where(eq("identifier", id)));
        // basic containment indexing
        if (container != null) {
            // the relationship between container and identifier is swapped
            Insert bcIndexInsert = insertInto("trellis", "basiccontainment").value("identifier", container)
                            .value("contained", id);
            CompletableFuture<Void> bcContainmentResult = execute(bcIndexInsert);
            Update.Where containerUpdate = update("trellis", Mutable.tableName).with(set("modified", now()))
                            .where(eq("identifier", container));
            return allOf(bcContainmentResult, execute(updateStatement), execute(containerUpdate));
        }
        return execute(updateStatement);
    }

    private CompletableFuture<Void> execute(Statement statement) {
        log.debug("Executing CQL statement: {}", statement);
        return translate(session.executeAsync(statement)).thenApply(r -> null);
    }

    private <T> CompletableFuture<T> translate(ListenableFuture<T> result) {
        return supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                // we don't know that persistence failed but we can't assume that it succeeded
                log.error("Error in persistence!", e.getCause());
                throw new CompletionException(e.getCause());
            }
        }, executor);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }

    protected <T> Optional<T> resynchronize(CompletableFuture<T> from) {
        try {
            return Optional.of(from.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }
}
