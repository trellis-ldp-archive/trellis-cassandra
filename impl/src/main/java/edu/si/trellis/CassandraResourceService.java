package edu.si.trellis;

import static java.time.Instant.now;
import static java.util.UUID.randomUUID;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Metadata.builder;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.api.TrellisUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.vocabulary.LDP.BasicContainer;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.RDFSource;
import static org.trellisldp.vocabulary.LDP.getSuperclassOf;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;

import edu.si.trellis.query.rdf.BasicContainment;
import edu.si.trellis.query.rdf.Delete;
import edu.si.trellis.query.rdf.Get;
import edu.si.trellis.query.rdf.ImmutableInsert;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MutableInsert;
import edu.si.trellis.query.rdf.Touch;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.Metadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.api.TrellisUtils;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
class CassandraResourceService extends CassandraBuildingService implements ResourceService {

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer);

    static final Logger log = getLogger(CassandraResourceService.class);

    private final Delete delete;

    private final Get get;

    private final ImmutableInsert immutableInsert;

    private final MutableInsert mutableInsert;

    private final Touch touch;

    private final BasicContainment bcontainment;

    private final ImmutableRetrieve immutableRetrieve;

    @Inject
    CassandraResourceService(Delete delete, Get get, ImmutableInsert immutableInsert,
                    MutableInsert mutableInsert, Touch touch,
                    ImmutableRetrieve immutableRetrieve, BasicContainment bcontainment) {
        this.delete = delete;
        this.get = get;
        this.immutableInsert = immutableInsert;
        this.mutableInsert = mutableInsert;
        this.touch = touch;
        this.immutableRetrieve = immutableRetrieve;
        this.bcontainment = bcontainment;

    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeRoot() {

        IRI rootIri = TrellisUtils.getInstance().createIRI(TRELLIS_DATA_PREFIX);
        try {
            if (get(rootIri).toCompletableFuture().get().equals(MISSING_RESOURCE)) {
                Metadata rootResource = builder(rootIri).interactionModel(BasicContainer).build();
                create(rootResource, null).toCompletableFuture().get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedStartupException("Interrupted while building repository root!", e);
        } catch (ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    @Override
    public CompletionStage<? extends Resource> get(final IRI id) {
        final Stream<Quad> immutable = immutableRetrieve.execute(id);

        CompletableFuture<Resource> resource = get.execute(id)
                        .thenApply(rows -> parse(rows, log, id))
                        .thenApply(res -> {
                            immutable.forEach(res.dataset()::add);
                            return res;
                        }).thenApply(res -> {
                            if (isContainer(res))
                                bcontainment.execute(id).forEach(res.dataset()::add);
                            return res;
                        });
        return resource;
    }

    private static boolean isContainer(Resource res) {
        return Container.equals(res.getInteractionModel())
                        || Container.equals(getSuperclassOf(res.getInteractionModel()));
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletionStage<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        return immutableInsert.execute(id, dataset, now());
    }

    @Override
    public CompletionStage<Void> create(Metadata meta, Dataset data) {
        log.debug("Creating {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletionStage<Void> replace(Metadata meta, Dataset data) {
        log.debug("Replacing {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletionStage<Void> delete(Metadata meta) {
        log.debug("Deleting {}", meta.getIdentifier());
        return delete.execute(meta.getIdentifier());
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletionStage<Void> touch(IRI id) {
        return touch.execute(now(), id);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }

    private CompletionStage<Void> write(Metadata meta, Dataset data) {
        IRI id = meta.getIdentifier();
        IRI ixnModel = meta.getInteractionModel();
        IRI container = meta.getContainer().orElse(null);

        Optional<BinaryMetadata> binary = meta.getBinary();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);
        Instant now = now();

        return mutableInsert.execute(ixnModel, mimeType, container, data, now, binaryIdentifier, UUIDs.timeBased(), id);
    }
}
