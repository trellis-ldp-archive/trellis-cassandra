package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import edu.si.trellis.MutableReadConsistency;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * Retrieve data for a Memento.
 */
public class GetMemento extends ResourceQuery {

    @Inject
    public GetMemento(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT * FROM " + MEMENTO_MUTABLE_TABLENAME
                        + " WHERE identifier = :identifier AND mementomodified <= :time " + " LIMIT 1 ALLOW FILTERING;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the Memento to retrieve
     * @param time the time for which this Memento is valid
     * @return the data for the Memento
     */
    public CompletionStage<AsyncResultSet> execute(IRI id, Instant time) {
        BoundStatement statement = preparedStatement().bind()
                        .set("time", time, Instant.class)
                        .set("identifier", id, IRI.class);
        return executeRead(statement);
    }
}
