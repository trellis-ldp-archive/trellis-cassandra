package edu.si.trellis.cassandra;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;

import com.datastax.driver.mapping.Mapper;

public class CassandraResourceService implements ResourceService {
	
	Mapper<CassandraResource> resourceManager;

	@Override
	public Stream<IRI> compact(IRI identifier, Instant from, Instant until) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<Resource> get(IRI identifier) {
		return Optional.ofNullable(resourceManager.get(identifier));
	}

	@Override
	public Optional<Resource> get(IRI identifier, Instant time) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Supplier<String> getIdentifierSupplier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<Triple> scan(String partition) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stream<IRI> purge(IRI identifier) {
		get(identifier).flatMap(Resource::getMembershipResource);
		resourceManager.delete(identifier);
		return null;
	}

	@Override
	public Boolean put(IRI identifier, Dataset quads) {
		return true;
	}
}
