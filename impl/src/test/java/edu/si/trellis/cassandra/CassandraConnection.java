package edu.si.trellis.cassandra;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static edu.si.trellis.cassandra.InputStreamCodec.inputStreamCodec;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;

class CassandraConnection implements AfterAllCallback, BeforeAllCallback {

    private static final String[] CLEANOUT_QUERIES = new String[] { "TRUNCATE Metadata ; ", "TRUNCATE Mutabledata ; ",
            "TRUNCATE Immutabledata ;", "TRUNCATE Binarydata ;" };

    private static final Logger log = getLogger(CassandraConnection.class);

    private static final String keyspace = "trellis";

    private Cluster cluster;

    private Session session;

    CassandraResourceService resourceService;

    CassandraBinaryService binaryService;

    private static final String contactAddress = System.getProperty("cassandra.contactAddress", "localhost");

    private static final Integer contactPort = Integer.getInteger("cassandra.nativeTransportPort", 9042);

    private static final boolean cleanBefore = Boolean.getBoolean("cleanBeforeTests");

    private static final boolean cleanAfter = Boolean.getBoolean("cleanAfterTests");

    @Override
    public void beforeAll(ExtensionContext context) {
        log.debug("Trying Cassandra connection at: {}:{}", contactAddress, contactPort);
        this.cluster = builder().withoutMetrics().addContactPoint(contactAddress).withPort(contactPort).build();
        codecRegistry().register(inputStreamCodec, iriCodec, datasetCodec, InstantCodec.instance,
                        SimpleTimestampCodec.instance);
        QueryLogger queryLogger = QueryLogger.builder().build();
        cluster.register(queryLogger);
        this.session = cluster.connect("trellis");
        this.resourceService = new CassandraResourceService(new ResourceQueryContext(session, ONE, ONE));
        resourceService.initializeQueriesAndRoot();
        this.binaryService = new CassandraBinaryService(null, 1024 * 1024, new BinaryQueryContext(session, ONE, ONE));
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
    public void afterAll(ExtensionContext context) {
        if (cleanAfter) cleanOut();
        session.close();
        cluster.close();
    }
}
