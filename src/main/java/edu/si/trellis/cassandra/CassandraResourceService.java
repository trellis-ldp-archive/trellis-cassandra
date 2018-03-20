package edu.si.trellis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Meta;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.StreamSupport.stream;
import static org.trellisldp.vocabulary.RDF.type;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.lang3.Range;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.api.Session;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService implements ResourceService {

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceService.class);

    private static final String[] DATA_COLUMNS = new String[] { "identifier", "quads" };
    private final com.datastax.driver.core.Session cassandraSession;

    private static final JenaRDF rdf = new JenaRDF();

    private static final String SCAN_QUERY = "SELECT identifier, interactionModel FROM " + Meta.tableName + " ;";

    /**
     * Scans the Cassandra cluster, for use by {@link #scan()}. Should normally
     * contain a bound form of {@value #SCAN_QUERY} drawn from {@link #SCAN_QUERY}.
     *
     * @see #SCAN_QUERY
     */
    private final BoundStatement scanStatement;

    private static final String CONTAINS_QUERY = "SELECT identifier FROM " + Meta.tableName + " WHERE identifier = ?;";

    private final PreparedStatement containsStatement;

    private static final String DELETE_QUERY = "DELETE FROM " + Mutable.tableName + " WHERE identifier = ?;";

    private final PreparedStatement deleteStatement;

    /**
     * Same-thread execution. TODO optimize with a threadpool
     */
    private final Executor executor = Runnable::run;

    /**
     * Constructor.
     *
     * @param session a Cassandra object mapper {@link Session} for use by this
     *            service for its lifetime
     */
    @Inject
    public CassandraResourceService(final com.datastax.driver.core.Session session) {
        this.cassandraSession = session;
        scanStatement = session.prepare(SCAN_QUERY).bind();
        containsStatement = session.prepare(CONTAINS_QUERY);
        deleteStatement = session.prepare(DELETE_QUERY);
    }

    @Override
    public Stream<IRI> compact(final IRI identifier, final Instant from, final Instant until) {
        // TODO do something with bitstreams
        return Stream.empty();
    }

    @Override
    public Optional<CassandraResource> get(final IRI id) {
        BoundStatement boundStatement = containsStatement.bind(id);
        boolean absent = cassandraSession.execute(boundStatement).one() == null;
        return ofNullable(absent ? null : new CassandraResource(id, cassandraSession));
    }

    @Override
    public Optional<CassandraResource> get(final IRI identifier, final Instant time) {
        // TODO versioning, but not today
        return get(identifier);
    }

    @Override
    public Optional<IRI> getContainer(final IRI id) {
        // TODO Java 9 fixes this with Optional::or
        return get(id).map(CassandraResource::getParent).map(Optional::of)
                        .orElseGet(() -> ResourceService.super.getContainer(id));
    }

    @Override
    public Stream<Triple> scan() {
        final Spliterator<Row> spliterator = cassandraSession.execute(scanStatement).spliterator();
        return stream(spliterator, false).map(row -> {
            final IRI resource = row.get("identifier", IRI.class);
            final IRI ixnModel = row.get("interactionModel", IRI.class);
            return rdf.createTriple(resource, type, ixnModel);
        });
    }

    @Override
    public Stream<IRI> purge(final IRI id) {
        BoundStatement boundStatement = deleteStatement.bind(id);
        cassandraSession.execute(boundStatement);
        // TODO do something with binaries!
        return Stream.empty();
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public List<Range<Instant>> getMementos(final IRI identifier) {
        // TODO maybe someone uses versioning?
        return emptyList();
    }

    @Override
    public Future<Boolean> add(final IRI id, Session session, final Dataset dataset) {
        Insert immutableDataInsert = insertInto(Immutable.tableName).values(DATA_COLUMNS, new Object[] { id, dataset });
        return execute(immutableDataInsert);
    }

    @Override
    public Future<Boolean> create(final IRI id, final Session session, final IRI ixnModel, final Dataset dataset) {
        return write(id, ixnModel, dataset);
    }

    @Override
    public Future<Boolean> replace(final IRI id, final Session session, final IRI ixnModel, final Dataset dataset) {
        return write(id, ixnModel, dataset);
    }

    @Override
    public Future<Boolean> delete(final IRI id, final Session session, final IRI ixnModel, final Dataset dataset) {
        return write(id, ixnModel, dataset);
    }

    enum Mutability {
        Mutable("Mutabledata"), Immutable("Immutabledata"), Meta("Metadata");

        private Mutability(String tName) {
            this.tableName = tName;
        }

        public final String tableName;
    }

    private Future<Boolean> write(final IRI id, final IRI ixnModel, final Dataset dataset) {
        Insert mutableDataInsert = insertInto(Mutable.tableName).values(DATA_COLUMNS, new Object[] { id, dataset });
        RegularStatement[] ops = new RegularStatement[] { mutableDataInsert, metadataInsert(id, ixnModel) };
        return execute(ops);
    }

    private Future<Boolean> execute(RegularStatement... statements) {
        return translate(cassandraSession.executeAsync(batch(statements)));
    }

    private RegularStatement metadataInsert(final IRI id, final IRI ixnModel) {
        return update(Meta.tableName).with(set("interactionModel", ixnModel)).where(eq("identifier", id));
    }

    private Future<Boolean> translate(ResultSetFuture result) {
        return supplyAsync(() -> {
            try {
                return result.get().wasApplied();
            } catch (InterruptedException | ExecutionException e) {
                // we don't know that persistence failed but we can't assume that it succeeded
                throw new RuntimeTrellisException(new CompletionException(e));
            }
        }, executor);
    }
}
