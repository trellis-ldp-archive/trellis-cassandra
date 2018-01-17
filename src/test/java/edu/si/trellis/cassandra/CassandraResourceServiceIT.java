package edu.si.trellis.cassandra;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.Resource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class CassandraResourceServiceIT extends Assert {

    private static final String TEST_KEYSPACE = System.getProperty("test.keyspace", "test");

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceServiceIT.class);

    protected int port = Integer.getInteger("cassandra.nativeTransportPort");
    protected Builder clusterBuilder = Cluster.builder().addContactPoint("127.0.0.1").withPort(port)
                    .withCodecRegistry(new CodecRegistry().register(new IRICodec(), new DatasetCodec(), InstantCodec.instance));

    protected RDF rdfFactory = new SimpleRDF();

    @Test
    public void test() throws InterruptedException, ExecutionException {
        try (Cluster cluster = clusterBuilder.build(); Session session = cluster.connect(TEST_KEYSPACE)) {

            Mapper<CassandraResource> resourceManager = new MappingManager(session).mapper(CassandraResource.class);
            CassandraResourceService service = new CassandraResourceService(resourceManager);
            IRI id = createIRI("http://example.com");
            IRI ixnModel = createIRI("http://example.com");
            Dataset quads = rdfFactory.createDataset();
            CompletableFuture<Boolean> put = service.put(id, ixnModel, quads);
            assertTrue(put.get());
            Resource resource = service.get(id).orElseThrow(() -> new AssertionError("Failed to retrieve resource!"));
            assertEquals(id, resource.getIdentifier());
            assertEquals(ixnModel, resource.getInteractionModel());
            assertEquals(0, resource.stream().count());
        } catch (NoHostAvailableException e) {
            fail("Couldn't find Cassandra because: " + e.getLocalizedMessage());
        }
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }
}
