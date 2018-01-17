package edu.si.trellis.cassandra;

import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;

import com.datastax.driver.mapping.Mapper;

public class CassandraResourceService implements ResourceService {

    @Inject
    private Mapper<CassandraResource> resourceManager;

    public CassandraResourceService(Mapper<CassandraResource> resourceManager) {
        this.resourceManager = resourceManager;
    }

    @Override
    public Stream<IRI> compact(IRI identifier, Instant from, Instant until) {
        return Stream.empty();
    }

    @Override
    public Optional<Resource> get(IRI identifier) {
        return Optional.ofNullable(resourceManager.get(identifier));
    }

    @Override
    public Optional<Resource> get(IRI identifier, Instant time) {
        return get(identifier);
    }

    @Override
    public Stream<Triple> scan() {
        //TODO?
        return null;
    }

    @Override
    public Stream<IRI> purge(IRI identifier) {
        resourceManager.delete(identifier);
        return Stream.empty();
    }

    @Override
    public CompletableFuture<Boolean> put(IRI id, IRI ixnModel, Dataset quads) {
        final CassandraResource resource = new CassandraResource(requireNonNull(id), requireNonNull(ixnModel), quads);
        resourceManager.save(resource);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Supplier<String> getIdentifierSupplier() {
        return () -> randomUUID().toString();
    }
}
