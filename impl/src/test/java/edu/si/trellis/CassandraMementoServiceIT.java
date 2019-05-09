package edu.si.trellis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.trellisldp.api.Metadata.builder;

import java.time.Instant;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.jupiter.api.Test;
import org.trellisldp.api.Metadata;

class CassandraMementoServiceIT extends CassandraServiceIT {

    @Test
    void mementos() throws InterruptedException, ExecutionException {
        IRI id = createIRI("http://example.com/id/foo2");
        IRI ixnModel = createIRI("http://example.com/ixnModel2");
        @SuppressWarnings("resource")
        Dataset quads = rdfFactory.createDataset();
        Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
        quads.add(quad);

        // build resource
        Metadata meta = builder(id).interactionModel(ixnModel).build();
        connection.resourceService.create(meta, quads).toCompletableFuture().get();
        connection.mementoService.put(connection.resourceService, id).toCompletableFuture().get();

        SortedSet<Instant> mementos = connection.mementoService.mementos(id).toCompletableFuture().get();
        assertEquals(1, mementos.size());
        waitTwoSeconds();

        // again
        connection.resourceService.replace(meta, quads).toCompletableFuture().get();
        connection.mementoService.put(connection.resourceService, id).toCompletableFuture().get();

        mementos = connection.mementoService.mementos(id).toCompletableFuture().get();
        assertEquals(2, mementos.size());
    }
}
