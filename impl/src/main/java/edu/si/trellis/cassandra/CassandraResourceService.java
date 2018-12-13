package edu.si.trellis.cassandra;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Metadata.builder;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.api.TrellisUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.vocabulary.LDP.BasicContainer;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.RDFSource;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.*;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService implements ResourceService, MementoService {

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    private final ResourceQueryContext cassandra;

    /**
     * Constructor.
     * 
     * @param queryContext the Cassandra context for queries
     */
    @Inject
    public CassandraResourceService(ResourceQueryContext queryContext) {
        this.cassandra = queryContext;
    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeQueriesAndRoot() {

        IRI rootIri = TrellisUtils.getInstance().createIRI(TRELLIS_DATA_PREFIX);
        try {
            if (get(rootIri).get().equals(MISSING_RESOURCE)) {
                Metadata rootResource = builder(rootIri).interactionModel(BasicContainer).build();
                create(rootResource, null).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    private Function<? super ResultSet, ? extends Resource> buildResource(IRI id) {
        return rows -> {
            final Row metadata = rows.one();
            boolean wasFound = metadata != null;
            log.debug("Resource {} was {}found", id, wasFound ? "" : "not ");
            if (!wasFound) return MISSING_RESOURCE;

            log.trace("Computing metadata for resource {}", id);
            IRI ixnModel = metadata.get("interactionModel", IRI.class);
            log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
            boolean hasAcl = metadata.getBool("hasAcl");
            log.debug("Found hasAcl = {} for resource {}", hasAcl, id);
            IRI binaryId = metadata.get("binaryIdentifier", IRI.class);
            log.debug("Found binaryIdentifier = {} for resource {}", binaryId, id);
            String mimeType = metadata.getString("mimetype");
            log.debug("Found mimeType = {} for resource {}", mimeType, id);
            long size = metadata.getLong("size");
            log.debug("Found size = {} for resource {}", size, id);
            IRI container = metadata.get("container", IRI.class);
            log.debug("Found container = {} for resource {}", container, id);
            Instant modified = metadata.get("modified", Instant.class);
            log.debug("Found modified = {} for resource {}", modified, id);
            UUID created = metadata.getUUID("created");
            log.debug("Found created = {} for resource {}", created, id);
            return new CassandraResource(id, ixnModel, hasAcl, binaryId, mimeType, size, container, modified, created,
                            cassandra);
        };
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        return get(id, now());
    }

    @Override
    public CompletableFuture<Resource> get(final IRI id, Instant time) {
        return cassandra.get(id, time).thenApply(buildResource(id));
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletableFuture<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        return cassandra.immutate(id, dataset, now());
    }

    @Override
    public CompletableFuture<Void> create(Metadata meta, Dataset data) {
        log.debug("Creating {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletableFuture<Void> replace(Metadata meta, Dataset data) {
        log.debug("Replacing {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletableFuture<Void> delete(Metadata meta) {
        log.debug("Deleting {}", meta.getIdentifier());
        return cassandra.delete(meta.getIdentifier());
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletableFuture<Void> touch(IRI id) {
        return get(id).thenApply(CassandraResource.class::cast).thenApply(CassandraResource::getCreated)
                        .thenCompose(created -> cassandra.touch(now(), created, id));
    }

    @Override
    public CompletableFuture<Void> put(Resource resource) {
        // NOOP see our data model
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<SortedSet<Instant>> mementos(IRI id) {
        return cassandra.mementos(id)
                        .thenApply(results -> stream(results::spliterator, NONNULL + DISTINCT, false)
                                        .map(r -> r.get("modified", Instant.class))
                                        .map(time -> time.truncatedTo(SECONDS)).collect(toCollection(TreeSet::new)));
    }

    private CompletableFuture<Void> write(Metadata meta, Dataset data) {
        IRI id = meta.getIdentifier();
        IRI ixnModel = meta.getInteractionModel();
        IRI container = meta.getContainer().orElse(null);
        Instant now = now();

        Optional<BinaryMetadata> binary = meta.getBinary();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        Long size = binary.flatMap(BinaryMetadata::getSize).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);

        return cassandra.mutate(ixnModel, size, mimeType, now.truncatedTo(SECONDS), container, data, now,
                        binaryIdentifier, UUIDs.timeBased(), id);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }
}
