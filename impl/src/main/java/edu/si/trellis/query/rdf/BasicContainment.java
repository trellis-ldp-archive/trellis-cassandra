package edu.si.trellis.query.rdf;

import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.contains;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.si.trellis.AsyncResultSetUtils;
import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;
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
    public BasicContainment(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT identifier AS contained FROM " + BASIC_CONTAINMENT_TABLENAME
                        + " WHERE container = :container ;", consistency);
    }

    /**
     * @param id the {@link IRI} of the container
     * @return a {@link ResultSet} of the resources contained in {@code id}
     */
    public CompletionStage<Stream<Quad>> execute(IRI id) {
        final BoundStatement query = preparedStatement().bind().set("container", id, IRI.class);
        return executeRead(query)
                        .thenApply(AsyncResultSetUtils::stream)
                        .thenApply(rows -> rows.map(this::getContained))
                        .thenApply(rows -> rows.map(con -> containmentQuad(id, con)));
    }

    private IRI getContained(Row r) {
        return r.get("contained", IRI.class);
    }

    private static Quad containmentQuad(IRI id, IRI con) {
        return rdfFactory.createQuad(PreferContainment, id, contains, con);
    }
}
