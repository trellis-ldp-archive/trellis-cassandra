package edu.si.trellis.cassandra;

import static com.datastax.driver.core.Cluster.builder;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static org.slf4j.LoggerFactory.getLogger;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.trellisldp.api.IdentifierService;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import org.trellisldp.id.UUIDGenerator;

class CassandraConnection extends ExternalResource {

    private static final Logger log = getLogger(CassandraConnection.class);

    private static final String KEYSPACE = "Trellis";

    protected Cluster cluster;
    protected Session session;
    public CassandraResourceService service;
    public CassandraBinaryService binaryService;
    private final int port;
    private final String contactLocation;

    public CassandraConnection(final String contactLocation, final int port) {
        this.contactLocation = contactLocation;
        this.port = port;
    }

    @Override
    protected void before() {
        cluster = builder().withoutMetrics().addContactPoint(contactLocation).withPort(port).build();
        codecRegistry().register(iriCodec, datasetCodec, InstantCodec.instance, TypeCodec.bigint());
        session = cluster.connect(KEYSPACE);
        service = new CassandraResourceService(session);
        // UUIDGenerator idService = new UUIDGenerator();
        binaryService = new CassandraBinaryService(null, session, 1 * 1024 * 1024);
    }

    private CodecRegistry codecRegistry() {
        return cluster.getConfiguration().getCodecRegistry();
    }

    @Override
    protected void after() {
        session.close();
        cluster.close();
    }
}
