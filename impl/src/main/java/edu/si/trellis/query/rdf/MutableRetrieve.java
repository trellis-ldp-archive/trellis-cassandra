package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfReadConsistency;

import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

public class MutableRetrieve extends ResourceQuery {

    @Inject
    public MutableRetrieve(Session session, @RdfReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + MUTABLE_TABLENAME
                        + "  WHERE identifier = ? AND createdSeconds <= ? LIMIT 1 ALLOW FILTERING;", consistency);
    }

    public Stream<Quad> execute(IRI id, Long time) {
        return quadStreamFromQuery(preparedStatement().bind(id, time));
    }
}
