package edu.si.trellis.cassandra;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.Resource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class CassandraResourceServiceIT extends Assert {

    private static final String TEST_KEYSPACE = System.getProperty("test.keyspace", "test");

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceServiceIT.class);

    protected static int port = Integer.getInteger("cassandra.nativeTransportPort");
    protected static Builder clusterBuilder = Cluster.builder().addContactPoint("127.0.0.1").withPort(port);

    protected static Cluster cluster;
    protected static Session session;
    protected RDF rdfFactory = new SimpleRDF();

    @BeforeClass
    public static void setUp() {
        cluster = clusterBuilder.build();
        cluster.getConfiguration().getCodecRegistry().register(new IRICodec(), new DatasetCodec(),
                        InstantCodec.instance);
        session = cluster.connect(TEST_KEYSPACE);
    }
    
    @AfterClass
    public static void tearDown() {
        session.close();
        cluster.close();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
            Mapper<CassandraResource> resourceManager = new MappingManager(session).mapper(CassandraResource.class);
            CassandraResourceService service = new CassandraResourceService(resourceManager);
            IRI id = createIRI("http://example.com/id");
            IRI ixnModel = createIRI("http://example.com/ixnModel");
            Dataset quads = rdfFactory.createDataset();
            Quad quad = rdfFactory.createQuad(id, ixnModel, id, ixnModel);
            quads.add(quad);
            CompletableFuture<Boolean> put = service.put(id, ixnModel, quads);
            assertTrue(put.get());
            Resource resource = service.get(id).orElseThrow(() -> new AssertionError("Failed to retrieve resource!"));
            assertEquals(id, resource.getIdentifier());
            assertEquals(ixnModel, resource.getInteractionModel());
            assertEquals(quad, resource.stream().findFirst().orElseThrow(()->new AssertionError("Failed to find quad!")));
 
    }

    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }
}
