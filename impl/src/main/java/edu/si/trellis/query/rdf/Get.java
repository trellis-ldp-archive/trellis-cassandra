package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableReadConsistency;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Retrieve data for a resource.
 */
public class Get extends ResourceQuery {

    @Inject
    public Get(Session session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT * FROM " + MUTABLE_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    public CompletableFuture<ResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
