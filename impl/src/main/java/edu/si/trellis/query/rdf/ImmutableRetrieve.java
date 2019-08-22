package edu.si.trellis.query.rdf;

import static java.util.stream.StreamSupport.stream;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

/**
 * A query to retrieve immutable data about a resource from Cassandra.
 */
public class ImmutableRetrieve extends ResourceQuery {

    @Inject
    public ImmutableRetrieve(Session session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + IMMUTABLE_TABLENAME + "  WHERE identifier = :identifier ;", consistency);
    }

    /**
     * @param id the {@link IRI} of the resource, the immutable data of which is to be retrieved
     * @return the RDF retrieved
     */
    public CompletionStage<Stream<Quad>> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("identifier", id, IRI.class))
                        .thenApply(ResultSet::spliterator)
                        .thenApply(r -> stream(r, false))
                        .thenApply(row -> row.map(this::getDataset))
                        .thenApply(r -> r.flatMap(Dataset::stream));
    }

    private Dataset getDataset(Row r) {
        return r.get("quads", Dataset.class);
    }
}
