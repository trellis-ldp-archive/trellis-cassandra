package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfReadConsistency;

import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

public class ImmutableRetrieve extends ResourceQuery {

    @Inject
    public ImmutableRetrieve(Session session, @RdfReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + IMMUTABLE_TABLENAME + "  WHERE identifier = ? ;", consistency);
    }

    public Stream<Quad> execute(IRI id) {
        return quadStreamFromQuery(preparedStatement().bind(id));
    }
}
