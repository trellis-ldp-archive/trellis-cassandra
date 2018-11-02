package edu.si.trellis.cassandra;

import static com.datastax.driver.core.Cluster.builder;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static edu.si.trellis.cassandra.InputStreamCodec.inputStreamCodec;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import java.util.concurrent.ExecutorService;

class CassandraConnection extends ExternalResource {

    private static final String[] CLEANOUT_QUERIES = new String[] { "TRUNCATE Metadata ; ", "TRUNCATE Mutabledata ; ",
            "TRUNCATE Immutabledata ;", "TRUNCATE Binarydata ;" };

    private static final Logger log = getLogger(CassandraConnection.class);

    private final String keyspace;

    protected Cluster cluster;
    protected Session session;
    public CassandraResourceService resourceService;
    public CassandraBinaryService binaryService;
    private final int port;
    private final String contactAddress;

    private final boolean cleanBefore, cleanAfter;

    public CassandraConnection(final String contactAddress, final int port, final String keyspace,
                    final boolean cleanBefore, final boolean cleanAfter) {
        this.contactAddress = contactAddress;
        this.port = port;
        this.keyspace = keyspace;
        this.cleanBefore = cleanBefore;
        this.cleanAfter = cleanAfter;
    }

    @Override
    protected void before() {
        cluster = builder().withoutMetrics().addContactPoint(contactAddress).withPort(port).build();
        codecRegistry().register(inputStreamCodec, iriCodec, datasetCodec, InstantCodec.instance);
        QueryLogger queryLogger = QueryLogger.builder().build();
        cluster.register(queryLogger);
        session = cluster.newSession();
        ExecutorService setKeyspaceThread = newSingleThreadExecutor();
        session.initAsync().addListener(() -> {
            session.execute("USE trellis;");
            setKeyspaceThread.shutdown();
        }, setKeyspaceThread);
        resourceService = new CassandraResourceService(() -> session);
        resourceService.initializeRoot();
        binaryService = new CassandraBinaryService(null, () -> session, 1 * 1024 * 1024);
        binaryService.initializeStatements();
        if (cleanBefore) cleanOut();
    }

    private void cleanOut() {
        log.info("Cleaning out test keyspace {}", keyspace);
        for (String q : CLEANOUT_QUERIES)
            session.execute(q);
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
