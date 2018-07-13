package edu.si.trellis.cassandra;

import static com.datastax.driver.core.Cluster.builder;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static edu.si.trellis.cassandra.InputStreamCodec.inputStreamCodec;
import static org.slf4j.LoggerFactory.getLogger;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

class CassandraConnection extends ExternalResource {

    private static final String[] CLEANOUT_QUERIES = new String[] { "TRUNCATE Metadata ; ", "TRUNCATE Mutabledata ; ",
            "TRUNCATE Immutabledata ;", "TRUNCATE Binarydata ;" };

    private static final Logger log = getLogger(CassandraConnection.class);

    private final String keyspace;

    protected Cluster cluster;
    protected Session session;
    public CassandraResourceService service;
    public CassandraBinaryService binaryService;
    private final int port;
    private final String contactLocation;

    private final boolean cleanBefore, cleanAfter;

    public CassandraConnection(final String contactLocation, final int port, final String keyspace,
                    final boolean cleanBefore, final boolean cleanAfter) {
        this.contactLocation = contactLocation;
        this.port = port;
        this.keyspace = keyspace;
        this.cleanBefore = cleanBefore;
        this.cleanAfter = cleanAfter;
    }

    @Override
    protected void before() {
        cluster = builder().withoutMetrics().addContactPoint(contactLocation).withPort(port).build();
        codecRegistry().register(inputStreamCodec, iriCodec, datasetCodec, InstantCodec.instance);
        session = cluster.connect(keyspace);
        service = new CassandraResourceService(session);
        // UUIDGenerator idService = new UUIDGenerator();
        binaryService = new CassandraBinaryService(null, session, 1 * 1024 * 1024);
        if (cleanBefore) cleanOut();
    }

    private void cleanOut() {
        log.info("Cleaning out test keyspace {}", keyspace);
        for (String q : CLEANOUT_QUERIES) session.execute(q);
    }

    private CodecRegistry codecRegistry() {
        return cluster.getConfiguration().getCodecRegistry();
    }

    @Override
    protected void after() {
        if (cleanAfter) cleanOut();
        session.close();
        cluster.close();
    }
}
