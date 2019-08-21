package edu.si.trellis.query.rdf;

import static java.util.stream.StreamSupport.stream;
import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.contains;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableReadConsistency;

import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.api.TrellisUtils;

/**
 * A query to retrieve basic containment information from a materialized view or index table.
 */
public class BasicContainment extends ResourceQuery {

    private static final RDF rdfFactory = TrellisUtils.getInstance();

    @Inject
    public BasicContainment(Session session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT identifier AS contained FROM " + BASIC_CONTAINMENT_TABLENAME
                        + " WHERE container = :container ;", consistency);
    }

    /**
     * @param id the {@link IRI} of the container
     * @return a {@link ResultSet} of the resources contained in {@code id}
     */
    public Stream<Quad> execute(IRI id) {
        final BoundStatement query = preparedStatement().bind().set("container", id, IRI.class);
        final Stream<Row> rows = stream(executeSyncRead(query).spliterator(), false);
        return rows.map(r -> r.get("contained", IRI.class)).map(con -> containmentQuad(id, con));
    }

    private static Quad containmentQuad(IRI id, IRI con) {
        return rdfFactory.createQuad(PreferContainment, id, contains, con);
    }
}
