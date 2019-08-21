package edu.si.trellis;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toCollection;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.utils.UUIDs;

import edu.si.trellis.query.rdf.GetFirstMemento;
import edu.si.trellis.query.rdf.GetMemento;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MementoMutableRetrieve;
import edu.si.trellis.query.rdf.Mementoize;
import edu.si.trellis.query.rdf.Mementos;

import java.time.Instant;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.MementoService;
import org.trellisldp.api.Resource;

/**
 * A {@link MementoService} that stores Mementos in a Cassandra table.
 *
 */
public class CassandraMementoService extends CassandraBuildingService implements MementoService {

    private static final Logger log = getLogger(CassandraMementoService.class);

    private final Mementos mementos;

    private final Mementoize mementoize;

    private final GetMemento getMemento;

    private final GetFirstMemento getFirstMemento;

    @Inject
    CassandraMementoService(Mementos mementos, Mementoize mementoize, GetMemento getMemento,
                    GetFirstMemento getFirstMemento) {
        this.mementos = mementos;
        this.mementoize = mementoize;
        this.getMemento = getMemento;
        this.getFirstMemento = getFirstMemento;
    }

    @Override
    public CompletionStage<Void> put(Resource r) {

        IRI id = r.getIdentifier();
        IRI ixnModel = r.getInteractionModel();
        IRI container = r.getContainer().orElse(null);
        Optional<BinaryMetadata> binary = r.getBinaryMetadata();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);
        Dataset data = r.dataset();
        Instant modified = r.getModified();
        UUID creation = UUIDs.timeBased();

        log.debug("Writing Memento for {} at time: {}", id, modified);
        return mementoize.execute(ixnModel, mimeType, container, data, modified, binaryIdentifier, creation, id);
    }

    //@formatter:off
    @Override
    public CompletionStage<SortedSet<Instant>> mementos(IRI id) {
        return mementos.execute(id).thenApply(
                        results -> results.all().stream()
                                        .map(row -> row.get("modified", Instant.class))
                                        .map(time -> time.truncatedTo(SECONDS))
                                        .collect(toCollection(TreeSet::new)));
    }
    //@formatter:on

    @Override
    public CompletionStage<Resource> get(final IRI id, Instant time) {
        log.debug("Retrieving Memento for: {} at {}", id, time);
        return getMemento.execute(id, time).thenCompose(
                        result -> result.isExhausted() ? getFirstMemento.execute(id) : completedFuture(result))
                        .thenApply(result -> parse(result, log, id));
    }
}
