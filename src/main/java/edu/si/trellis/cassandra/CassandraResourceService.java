package edu.si.trellis.cassandra;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.runAsync;

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
	private Mapper<RDFSource> resourceManager;

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
		return null;
	}

	@Override
	public Stream<IRI> purge(IRI identifier) {
		resourceManager.delete(identifier);
		return Stream.empty();
	}

    @Override
    public CompletableFuture<Boolean> put(IRI id, IRI ixnModel, Dataset quads) {
        return runAsync(() -> resourceManager.save(new RDFSource(id, quads)), Runnable::run).thenApply(x -> true);
    }

    @Override
    public Supplier<String> getIdentifierSupplier() {
        return () -> randomUUID().toString();
    }
}
