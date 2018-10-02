package edu.si.trellis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.RDFUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.vocabulary.LDP.*;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

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

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer, DirectContainer, IndirectContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    private final Session session;

    private static final String GET_QUERY = "SELECT identifier FROM trellis." + Mutable.tableName
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
        return translate(result).thenApply(rows -> {
            log.debug("Resource {} was {}found", id, rows.isExhausted() ? "not " : "");
            return rows.isExhausted() ? MISSING_RESOURCE : new CassandraResource(id, session);
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
        return write(id, ixnModel, null, null);
    }

    enum Mutability {
        Mutable("mutabledata"), Immutable("immutabledata");

        private Mutability(String tName) {
            this.tableName = tName;
        }

        public final String tableName;
    }

    private CompletableFuture<Void> write(final IRI id, final IRI ixnModel, final IRI container, final Dataset dataset) {
        RegularStatement updateStatement = dataset.getGraph(PreferServerManaged).map(serverManaged -> {
            // if this has a binary/bitstream, pick up the extra metadata therefor
            if (NonRDFSource.equals(ixnModel)) {
                log.debug("Detected resource has NonRDFSource type: {}", ixnModel);
                IRI binaryIdentifier = serverManaged.stream(id, DC.hasPart, null).map(Triple::getObject)
                                .map(IRI.class::cast).findFirst().orElseThrow(() -> new RuntimeTrellisException(
                                                "Binary persisted with no bitstream IRI!"));
                long size = serverManaged.stream(binaryIdentifier, DC.extent, null).map(Triple::getObject)
                                .map(Literal.class::cast).map(Literal::getLexicalForm).map(Long::parseLong).findFirst()
                                .orElse(0L);
                String mimeType = serverManaged.stream(binaryIdentifier, DC.format, null).map(Triple::getObject)
                                .map(Literal.class::cast).map(Literal::getLexicalForm).findFirst()
                                .orElse("application/octet-stream");
                log.debug("Persisting a NonRDFSource at {} with bitstream at {} of size {} and mimeType {}.", id,
                                binaryIdentifier, size, mimeType);
                return update("trellis", Mutable.tableName).with(set("interactionModel", ixnModel))
                                .and(set("size", size)).and(set("mimeType", mimeType))
                                .and(set("container", container))
                                .and(set("binaryIdentifier", binaryIdentifier)).where(eq("identifier", id));
            }
            // or else it's not a binary
            log.debug("Resource is RDF Source.");
            return null;
            // so just create RDFSource update statement
        }).orElse(update("trellis", Mutable.tableName).with(set("interactionModel", ixnModel))
                        .and(set("quads", dataset)).and(set("container", container)).where(eq("identifier", id)));
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
