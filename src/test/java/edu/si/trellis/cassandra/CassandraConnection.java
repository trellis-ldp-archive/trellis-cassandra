package edu.si.trellis.cassandra;

import static com.datastax.driver.core.Cluster.builder;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;

import org.junit.rules.ExternalResource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

class CassandraConnection extends ExternalResource {

    static final String TEST_KEYSPACE = System.getProperty("test.keyspace", "test");


    protected Cluster cluster;
    protected Session session;
    public CassandraResourceService service;
    private final int port;
    private final String contact;

    public CassandraConnection(String contact, int port) {
        this.contact = contact;
        this.port = port;
    }

    @Override
    protected void before() {
        cluster = builder().addContactPoint(contact).withPort(port).build();
        cluster.getConfiguration().getCodecRegistry().register(iriCodec, datasetCodec, InstantCodec.instance);
        session = cluster.connect(TEST_KEYSPACE);
        service = new CassandraResourceService(session);
    }

    @Override
    protected void after() {
        session.close();
        cluster.close();
    }
}