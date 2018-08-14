package edu.si.trellis.cassandra;

import static org.trellisldp.vocabulary.RDF.type;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.Triple;
import org.junit.Test;
import org.trellisldp.api.Resource;

public class CassandraResourceServiceIT extends CassandraServiceIT {

    @Test
    public void testCreateAndGet() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id/foo");
        IRI container = createIRI("http://example.com/id");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);
        Future<Boolean> put = connection.service.create(id, null, ixnModel, quads, container, null);
        assertTrue(put.get());
        Resource resource = connection.service.get(id).get();
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        Quad firstQuad = resource.stream().findFirst().orElseThrow(() -> new AssertionError("Failed to find quad!"));
        assertEquals(quad, firstQuad);
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }
}
