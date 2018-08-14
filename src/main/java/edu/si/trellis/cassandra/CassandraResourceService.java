package edu.si.trellis.cassandra;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Meta;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.supplyAsync;
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
import org.apache.commons.rdf.jena.JenaRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.Binary;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.api.Session;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.LDP;
import org.trellisldp.vocabulary.Trellis;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.ImmutableSet;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService extends CassandraService implements ResourceService {

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, LDP.RDFSource, LDP.NonRDFSource, LDP.Container,
                    LDP.BasicContainer, LDP.DirectContainer, LDP.IndirectContainer);

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceService.class);

    private static final String[] DATA_COLUMNS = new String[] { "identifier", "quads" };
    private final com.datastax.driver.core.Session cassandraSession;

    private static final JenaRDF rdf = new JenaRDF();

    private static final String GET_QUERY = "SELECT identifier FROM " + Meta.tableName + " WHERE identifier = ?;";

    private final PreparedStatement getStatement;

    /**
     * Same-thread execution. TODO optimize with a threadpool?
     */
    private final Executor executor = Runnable::run;

    /**
     * Constructor.
     *
     * @param session a Cassandra object mapper {@link Session} for use by this  service for its lifetime
     */
    @Inject
    public CassandraResourceService(final com.datastax.driver.core.Session session) {
        this.cassandraSession = session;
        getStatement = session.prepare(GET_QUERY);
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        BoundStatement boundStatement = getStatement.bind(id);
        ResultSetFuture result = cassandraSession.executeAsync(boundStatement);
        return translateRead(result).thenApply(
                        rows -> rows.isExhausted() ? MISSING_RESOURCE : new CassandraResource(id, cassandraSession));
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
    public CompletableFuture<Boolean> add(final IRI id, Session session, final Dataset dataset) {
        Insert immutableDataInsert = insertInto(Immutable.tableName).values(DATA_COLUMNS, new Object[] { id, dataset });
        return execute(immutableDataInsert);
    }

    @Override
    public CompletableFuture<Boolean> create(IRI id, Session session, IRI ixnModel, Dataset dataset, IRI container, Binary binary) {
        return write(id, ixnModel, dataset);
    }

    @Override
    public CompletableFuture<Boolean> replace(final IRI id, final Session session, final IRI ixnModel, final Dataset dataset, final IRI container, final Binary binary) {
        return write(id, ixnModel, dataset);
    }

    @Override
    public CompletableFuture<Boolean> delete(final IRI id, final Session session, final IRI ixnModel, final Dataset dataset) {
        return write(id, ixnModel, dataset);
    }

    enum Mutability {
        Mutable("Mutabledata"), Immutable("Immutabledata"), Meta("Metadata");

        private Mutability(String tName) {
            this.tableName = tName;
        }

        public final String tableName;
    }

    private CompletableFuture<Boolean> write(final IRI id, final IRI ixnModel, final Dataset dataset) {

        Insert mutableDataInsert = insertInto(Mutable.tableName).values(DATA_COLUMNS, new Object[] { id, dataset });

        final RegularStatement metadataInsert = metadataInsert(id, ixnModel, dataset);
        RegularStatement[] ops = new RegularStatement[] { mutableDataInsert, metadataInsert };
        return execute(ops);
    }

    private static RegularStatement metadataInsert(final IRI id, final IRI ixnModel, final Dataset dataset) {
        return dataset.getGraph(Trellis.PreferServerManaged).map(serverManaged -> {
            // if this has a binary/bitstream, pick up the extra metadata therefor
            if (LDP.NonRDFSource.equals(ixnModel)) {
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
                return metadataForBinaryInsert(id, ixnModel, binaryIdentifier, size, mimeType);
            }
            // it's not a binary
            return null;
            // so just create RDFSource metadata
        }).orElse(metadataInsert(id, ixnModel));
    }

    private static RegularStatement metadataInsert(final IRI id, final IRI ixnModel) {
        return update(Meta.tableName).with(set("interactionModel", ixnModel)).where(eq("identifier", id));
    }

    private static RegularStatement metadataForBinaryInsert(final IRI id, final IRI ixnModel, IRI binaryIdentifier,
                    long size, String mimeType) {
        return update(Meta.tableName).with(set("interactionModel", ixnModel)).and(set("size", size))
                        .and(set("mimeType", mimeType)).and(set("binaryIdentifier", binaryIdentifier))
                        .where(eq("identifier", id));
    }

    private CompletableFuture<Boolean> execute(RegularStatement... statements) {
        return translateWrite(cassandraSession.executeAsync(batch(statements)));
    }

    private CompletableFuture<Boolean> translateWrite(ResultSetFuture result) {
        return supplyAsync(() -> {
            try {
                return result.get().wasApplied();
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
}
