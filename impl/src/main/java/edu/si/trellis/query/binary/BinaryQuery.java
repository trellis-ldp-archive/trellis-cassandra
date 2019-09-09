package edu.si.trellis.query.binary;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.query.CassandraQuery;

abstract class BinaryQuery extends CassandraQuery {

    BinaryQuery(CqlSession session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
    }
    
    static final String BINARY_TABLENAME = "binarydata";
}
