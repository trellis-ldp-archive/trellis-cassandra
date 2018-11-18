package edu.si.trellis.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.trellisldp.api.Metadata.builder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.jupiter.api.Test;
import org.trellisldp.api.Metadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.test.ResourceServiceTests;

public class CassandraResourceServiceIT extends CassandraServiceIT implements ResourceServiceTests {

    @Override
    public void testDeleteResource() {
        // TODO https://github.com/trellis-ldp/trellis/issues/303
    }

    @Test
    public void testCreateAndGet() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id/foo");
        IRI container = createIRI("http://example.com/id");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        @SuppressWarnings("resource")
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);
        Metadata meta = builder(id).interactionModel(ixnModel).container(container).build();
        Future<Void> put = connection.resourceService.create(meta, quads);
        put.get();
        assertTrue(put.isDone());
        Resource resource = connection.resourceService.get(id).get();
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        assertEquals(container,
                        resource.getContainer().orElseThrow(() -> new AssertionError("Failed to find any container!")));

        Quad firstQuad = resource.stream().findFirst().orElseThrow(() -> new AssertionError("Failed to find quad!"));
        assertEquals(quad, firstQuad);
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }

    @Override
    public ResourceService getResourceService() {
        return connection.resourceService;
    }
}
