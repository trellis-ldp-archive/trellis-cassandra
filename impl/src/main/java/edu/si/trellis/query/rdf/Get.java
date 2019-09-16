package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Retrieve data for a resource.
 */
public class Get extends ResourceQuery {

    @Inject
    public Get(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT * FROM " + MUTABLE_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    public CompletionStage<AsyncResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
