package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import edu.si.trellis.query.CassandraQuery;

import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Quad;

/**
 * A query for use by individual resources to retrieve their contents.
 *
 */
abstract class ResourceQuery extends CassandraQuery {

    ResourceQuery(Session session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
    }

    protected Stream<Quad> quads(final Statement boundStatement) {
        final Spliterator<Row> rows = executeSyncRead(boundStatement).spliterator();
        Stream<Dataset> datasets = StreamSupport.stream(rows, false).map(r -> r.get("quads", Dataset.class));
        return datasets.flatMap(Dataset::stream);
    }
}
