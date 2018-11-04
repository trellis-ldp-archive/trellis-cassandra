package edu.si.trellis.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static edu.si.trellis.cassandra.CassandraResourceService.MUTABLE_TABLENAME;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;

import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.enterprise.inject.Alternative;
import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.MementoService;
import org.trellisldp.api.Resource;

@Alternative
public class CassandraMementoService extends CassandraService implements MementoService {

    /**
     * Constructor.
     *
     * @param session a Cassandra {@link Session} for use by this service for its lifetime
     */
    @Inject
    public CassandraMementoService(final Session session) {
        super(session);
    }

    @Override
    public CompletableFuture<Void> put(IRI identifier, Instant time, Stream<? extends Quad> data) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<Resource> get(IRI identifier, Instant time) {
        // TODO Auto-generated method stub
        return completedFuture(MISSING_RESOURCE);
    }

    @Override
    public CompletableFuture<SortedSet<Instant>> mementos(IRI id) {
        Where query = select("modified").from(MUTABLE_TABLENAME).where(eq("identifier", id));
        return read(query).thenApply(results -> {
            Stream<Row> rows = stream(results::spliterator, NONNULL + DISTINCT, false);
            return rows.map(getFieldAs("modified", Instant.class)).collect(toCollection(TreeSet::new));
        });
    }

    private static <T> Function<Row, T> getFieldAs(String k, Class<T> klass) {
        return row -> row.get(k, klass);
    }

    @Override
    public CompletableFuture<Void> delete(IRI id, Instant time) {
        Statement delete = QueryBuilder.delete().from(MUTABLE_TABLENAME).where(eq("identifier", id))
                        .and(eq("modified", time));
        return execute(delete);
    }

}
