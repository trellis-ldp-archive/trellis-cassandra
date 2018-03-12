package edu.si.trellis.cassandra;

import static org.trellisldp.vocabulary.RDF.type;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.Resource;

public class CassandraResourceServiceIT extends Assert {

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceServiceIT.class);

    protected static int port = Integer.getInteger("cassandra.nativeTransportPort");

    protected RDF rdfFactory = new SimpleRDF();

    @ClassRule
    public static final CassandraConnection connection = new CassandraConnection("127.0.0.1", port);
 
    @Test
    public void testCreateAndGet() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);
        Future<Boolean> put = connection.service.create(id, null, ixnModel, quads);
        assertTrue(put.get());
        Resource resource = connection.service.get(id).orElseThrow(missing());
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        Quad firstQuad = resource.stream().findFirst().orElseThrow(missing("Failed to find quad!"));
        assertEquals(quad, firstQuad);
    }

    @Test
    public void testScan() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id2");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        connection.service.create(id, null, ixnModel, rdfFactory.createDataset()).get();
        assertEquals(1, connection.service.scan().count());
        Triple triple = connection.service.scan().findFirst().orElseThrow(missing());
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
        Future<Boolean> put = connection.service.create(id, null, ixnModel, quads);
        assertTrue(put.get());
        Instant longAgo = Instant.ofEpochMilli(-(10 ^ 10));
        Resource resource = connection.service.get(id, longAgo).orElseThrow(missing());
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        Quad firstQuad = resource.stream().findFirst().orElseThrow(missing("Failed to find quad!"));
        assertEquals(quad, firstQuad);
    }

    @Test
    public void testCompact() {
        // compact does nothing TODO
        assertFalse(connection.service.compact(null, null, null).findFirst().isPresent());
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }

    private static final Supplier<Error> missing(String... msg) {
        return msg.length == 0 ? missing("Failed to retrieve resource!") : () -> new AssertionError(msg[0]);
    }

}
