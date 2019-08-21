package edu.si.trellis.query.rdf;

import static java.util.stream.StreamSupport.stream;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import edu.si.trellis.query.CassandraQuery;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Quad;

/**
 * A query for use by individual resources to retrieve their contents.
 */
abstract class ResourceQuery extends CassandraQuery {

    static final String MUTABLE_TABLENAME = "mutabledata";

    static final String MEMENTO_MUTABLE_TABLENAME = "mementodata";

    static final String IMMUTABLE_TABLENAME = "immutabledata";

    static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    ResourceQuery(Session session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
    }

    protected CompletionStage<Stream<Quad>> quads(final Statement boundStatement) {
        return executeRead(boundStatement)
                        .thenApply(ResultSet::spliterator)
                        .thenApply(r -> stream(r, false))
                        .thenApply(row -> row.map(this::getDataset))
                        .thenApply(r -> r.flatMap(Dataset::stream));
    }

    private Dataset getDataset(Row r) {
        return r.get("quads", Dataset.class);
    }
}
