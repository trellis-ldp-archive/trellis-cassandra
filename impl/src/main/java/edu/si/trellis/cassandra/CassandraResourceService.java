package edu.si.trellis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.Trellis.PreferServerManaged;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService implements ResourceService {

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, LDP.RDFSource,
                    LDP.NonRDFSource, LDP.Container, LDP.BasicContainer, LDP.DirectContainer, LDP.IndirectContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    private static final String[] DATA_COLUMNS = new String[] { "identifier", "quads", "interactionModel", "mimeType",
            "parent" };
    private final Session session;

    private static final String GET_QUERY = "SELECT identifier FROM trellis." + Mutable.tableName + " WHERE identifier = ?;";

    private final PreparedStatement getStatement;

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
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        BoundStatement boundStatement = getStatement.bind(id);
        ResultSetFuture result = session.executeAsync(boundStatement);
        return translateRead(result)
                        .thenApply(rows -> rows.isExhausted() ? MISSING_RESOURCE : new CassandraResource(id, session));
    }

    @Override
    public Optional<IRI> getContainer(final IRI id) {
        return resynchronize(get(id)).flatMap(r -> r instanceof CassandraResource ? ((CassandraResource) r).getParent()
                        : ResourceService.super.getContainer(id));
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletableFuture<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        Insert immutableDataInsert = insertInto(Immutable.tableName).values(DATA_COLUMNS, new Object[] { id, dataset });
        return execute(immutableDataInsert);
    }

    @Override
    public CompletableFuture<Void> create(IRI id, IRI ixnModel, Dataset dataset, IRI container, Binary binary) {
        log.debug("Creating {} with interaction model {}", id, ixnModel);
        return write(id, ixnModel, dataset);
    }

    @Override
    public CompletableFuture<Void> replace(final IRI id, final IRI ixnModel, final Dataset dataset, final IRI container,
                    final Binary binary) {
        log.debug("Replacing {} with interaction model {}", id, ixnModel);
        return write(id, ixnModel, dataset);
    }

    @Override
    public CompletableFuture<Void> delete(final IRI id, final IRI ixnModel) {
        log.debug("Deleting {} with interaction model {}", id, ixnModel);
        return write(id, ixnModel, null);
    }

    enum Mutability {
        Mutable("mutabledata"), Immutable("immutabledata");

        private Mutability(String tName) {
            this.tableName = tName;
        }

        public final String tableName;
    }

    private CompletableFuture<Void> write(final IRI id, final IRI ixnModel, final Dataset dataset) {
        RegularStatement updateStatement = dataset.getGraph(PreferServerManaged).map(serverManaged -> {
            // if this has a binary/bitstream, pick up the extra metadata therefor
            if (NonRDFSource.equals(ixnModel)) {
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
                                .and(set("binaryIdentifier", binaryIdentifier)).where(eq("identifier", id));
            }
            // or else it's not a binary
            return null;
            // so just create RDFSource update statement
        }).orElse(update("trellis", Mutable.tableName).with(set("interactionModel", ixnModel))
                        .and(set("quads", dataset)).where(eq("identifier", id)));
        return execute(updateStatement);
    }

    private CompletableFuture<Void> execute(RegularStatement statement) {
        return translateWrite(session.executeAsync(statement));
    }

    private CompletableFuture<Void> translateWrite(ResultSetFuture result) {
        return runAsync(() -> {
            try {
                result.get();
            } catch (InterruptedException | ExecutionException e) {
                // we don't know that persistence failed but we can't assume that it succeeded
                throw new RuntimeTrellisException(new CompletionException(e));
            }
        }, executor);
    }

    private CompletableFuture<ResultSet> translateRead(ResultSetFuture result) {
        return supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeTrellisException(new CompletionException(e));
            }
        }, executor);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }

    protected <T> Optional<T> resynchronize(CompletableFuture<T> from) {
        // TODO https://github.com/trellis-ldp/trellis/issues/148
        try {
            return Optional.of(from.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }
}
