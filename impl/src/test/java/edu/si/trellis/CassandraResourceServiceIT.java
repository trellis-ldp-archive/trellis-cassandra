package edu.si.trellis;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.TWO_SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.trellisldp.api.Metadata.builder;

import java.time.Instant;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.jupiter.api.Test;
import org.trellisldp.api.Metadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.test.ResourceServiceTests;

public class CassandraResourceServiceIT extends CassandraServiceIT implements ResourceServiceTests {

    @Test
    public void basicActions() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id/foo");
        IRI container = createIRI("http://example.com/id");
        IRI ixnModel = createIRI("http://example.com/ixnModel");
        @SuppressWarnings("resource")
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);

        // build container
        Metadata meta = builder(container).interactionModel(ixnModel).container(null).build();
        connection.resourceService.create(meta, null).get();

        // build resource
        meta = builder(id).interactionModel(ixnModel).container(container).build();
        connection.resourceService.create(meta, quads).get();

        Resource resource = connection.resourceService.get(id).get();
        assertEquals(id, resource.getIdentifier());
        assertEquals(ixnModel, resource.getInteractionModel());
        assertEquals(container,
                        resource.getContainer().orElseThrow(() -> new AssertionError("Failed to find any container!")));

        Quad firstQuad = resource.stream().findFirst().orElseThrow(() -> new AssertionError("Failed to find quad!"));
        assertEquals(quad, firstQuad);

        // touch container
        Instant modified = connection.resourceService.get(container).get().getModified();
        waitTwoSeconds();
        connection.resourceService.touch(container).get();
        Instant newModified = connection.resourceService.get(container).get().getModified();
        assertTrue(modified.compareTo(newModified) < 0);
    }

    @Test
    public void mementos() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id/foo2");
        IRI ixnModel = createIRI("http://example.com/ixnModel2");
        @SuppressWarnings("resource")
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);

        // build resource
        Metadata meta = builder(id).interactionModel(ixnModel).build();
        connection.resourceService.create(meta, quads).get();
        
        SortedSet<Instant> mementos = connection.resourceService.mementos(id).get();
        assertEquals(1, mementos.size());
        waitTwoSeconds();
        
        // again
        connection.resourceService.replace(meta, quads).get();

        mementos = connection.resourceService.mementos(id).get();
        assertEquals(2, mementos.size());
    }

    private void waitTwoSeconds() {
        await().pollDelay(TWO_SECONDS).until(() -> true);
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }

    @Override
    public ResourceService getResourceService() {
        return connection.resourceService;
    }
}
