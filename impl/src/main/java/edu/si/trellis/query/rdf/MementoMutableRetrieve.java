package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfReadConsistency;

import java.time.Instant;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

public class MementoMutableRetrieve extends ResourceQuery {

    @Inject
    public MementoMutableRetrieve(Session session, @RdfReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + MEMENTO_MUTABLE_TABLENAME
                        + " WHERE identifier = :identifier AND mementomodified <= :time " + "LIMIT 1 ALLOW FILTERING;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the Memento, the RDF of which is to be retrieved
     * @param time the time for which this Memento must be valid
     * @return the RDF retrieved
     */
    public Stream<Quad> execute(IRI id, Instant time) {
        return quads(preparedStatement().bind().bind()
                        .set("time", time, Instant.class)
                        .set("identifier", id,
                        IRI.class));
    }
}
