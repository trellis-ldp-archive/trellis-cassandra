package edu.si.trellis.query.rdf;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.query.CassandraQuery;

/**
 * A query for use by individual resources to retrieve their contents.
 */
abstract class ResourceQuery extends CassandraQuery {

    static final String MUTABLE_TABLENAME = "mutabledata";

    static final String MEMENTO_MUTABLE_TABLENAME = "mementodata";

    static final String IMMUTABLE_TABLENAME = "immutabledata";

    static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    ResourceQuery(CqlSession session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
    }
}
