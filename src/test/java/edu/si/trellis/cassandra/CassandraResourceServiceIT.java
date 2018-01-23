package edu.si.trellis.cassandra;

import static org.trellisldp.vocabulary.RDF.type;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.Resource;

public class CassandraResourceServiceIT extends Assert {

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceServiceIT.class);

    protected static int port = Integer.getInteger("cassandra.nativeTransportPort");

    protected RDF rdfFactory = new SimpleRDF();

    @ClassRule
    public static final CassandraConnection cassandraConnection = new CassandraConnection("127.0.0.1", port);

    @Test
    public void testPutAndGet() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);
        CompletableFuture<Boolean> put = cassandraConnection.service.put(id, ixnModel, quads);
        assertTrue(put.get());
        Resource resource = cassandraConnection.service.get(id).orElseThrow(MISSING_RESOURCE);
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        Quad firstQuad = resource.stream().findFirst().orElseThrow(() -> new AssertionError("Failed to find quad!"));
        assertEquals(quad, firstQuad);
    }

    @Test
    public void testScan() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id2");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        cassandraConnection.service.put(id, ixnModel, rdfFactory.createDataset()).get();
        assertEquals(1, cassandraConnection.service.scan().count());
        Triple triple = cassandraConnection.service.scan().findFirst().orElseThrow(MISSING_RESOURCE);
        assertEquals(id, triple.getSubject());
        assertEquals(type, triple.getPredicate());
        assertEquals(ixnModel, triple.getObject());
    }
    
    @Test
    public void testGetWithTime() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id3");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);
        CompletableFuture<Boolean> put = cassandraConnection.service.put(id, ixnModel, quads);
        assertTrue(put.get());
        Resource resource = cassandraConnection.service.get(id, Instant.ofEpochMilli(-10^10)).orElseThrow(MISSING_RESOURCE);
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        Quad firstQuad = resource.stream().findFirst().orElseThrow(() -> new AssertionError("Failed to find quad!"));
        assertEquals(quad, firstQuad);
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }

    private static final Supplier<Error> MISSING_RESOURCE = () -> new AssertionError("Failed to retrieve resource!");

}
