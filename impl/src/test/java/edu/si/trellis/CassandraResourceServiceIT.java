package edu.si.trellis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.trellisldp.api.Metadata.builder;

import java.time.Instant;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.jupiter.api.Test;
import org.trellisldp.api.Metadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.test.ResourceServiceTests;

class CassandraResourceServiceIT extends CassandraServiceIT implements ResourceServiceTests {

    @Test
    void basicActions() {
        IRI id = createIRI("http://example.com/id/foo");
        IRI container = createIRI("http://example.com/id");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        @SuppressWarnings("resource")
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);

        // build container
        Metadata meta = builder(container).interactionModel(ixnModel).container(null).build();
        connection.resourceService.create(meta, null).toCompletableFuture().join();

        // build resource
        meta = builder(id).interactionModel(ixnModel).container(container).build();
        connection.resourceService.create(meta, quads).toCompletableFuture().join();

        Resource resource = connection.resourceService.get(id).toCompletableFuture().join();
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        assertEquals(container,
                        resource.getContainer().orElseThrow(() -> new AssertionError("Failed to find any container!")));

        Quad firstQuad = resource.stream().findFirst().orElseThrow(() -> new AssertionError("Failed to find quad!"));
        assertEquals(quad, firstQuad);

        // touch container
        Instant modified = connection.resourceService.get(container).toCompletableFuture().join().getModified();
        waitTwoSeconds();
        connection.resourceService.touch(container).toCompletableFuture().join();
        Instant newModified = connection.resourceService.get(container).toCompletableFuture().join().getModified();
        assertTrue(modified.compareTo(newModified) < 0);
    }

    @Override
    public ResourceService getResourceService() {
        return connection.resourceService;
    }
}
