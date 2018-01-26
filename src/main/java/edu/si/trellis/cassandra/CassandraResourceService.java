package edu.si.trellis.cassandra;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.StreamSupport.stream;
import static org.trellisldp.vocabulary.RDF.type;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.lang3.Range;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Transient;

public class CassandraResourceService implements ResourceService {

    private static final String SCAN_QUERY = "SELECT identifier, interactionModel FROM Resource;";

    private Mapper<CassandraResource> resourceManager;

    private Session session;

    private static final JenaRDF rdf = new JenaRDF();

    private final BoundStatement scanStatement;

    @Inject
    public CassandraResourceService(Session session) {
        this.session = session;
        this.resourceManager = new MappingManager(session).mapper(CassandraResource.class);
        scanStatement = session.prepare(SCAN_QUERY).bind();
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
    public Optional<IRI> getContainer(IRI identifier) {
        return get(identifier).map(CassandraResource.class::cast).map(CassandraResource::parent);
    }

    @Override
    public Stream<Triple> scan() {
        Spliterator<Row> spliterator = session.execute(scanStatement).spliterator();
        return stream(spliterator, false).map(row -> {
            IRI resource = row.get("identifier", IRI.class);
            IRI ixnModel = row.get("interactionModel", IRI.class);
            return rdf.createTriple(resource, type, ixnModel);
        });
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
        return randomUUID()::toString;
    }

    @Transient
    @Override
    public List<Range<Instant>> getMementos(IRI identifier) {
        return emptyList();
    }
}
