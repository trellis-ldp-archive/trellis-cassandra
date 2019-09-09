package edu.si.trellis.query.rdf;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Retrieve data for the first Memento of a resource.
 */
public class GetFirstMemento extends ResourceQuery {

    @Inject
    public GetFirstMemento(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT * FROM " + MEMENTO_MUTABLE_TABLENAME + " WHERE identifier = :identifier LIMIT 1 ;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the Memento to retrieve
     * @return the data for the Memento
     */
    public CompletionStage<AsyncResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
