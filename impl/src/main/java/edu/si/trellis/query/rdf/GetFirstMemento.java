package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Retrieve data for the first Memento of a resource.
 */
public class GetFirstMemento extends ResourceQuery {

    @Inject
    public GetFirstMemento(Session session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT * FROM " + MEMENTO_MUTABLE_TABLENAME + " WHERE identifier = :identifier LIMIT 1 ;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the Memento to retrieve
     * @return the data for the Memento
     */
    public CompletionStage<ResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
