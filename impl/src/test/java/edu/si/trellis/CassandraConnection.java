package edu.si.trellis;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static edu.si.trellis.DatasetCodec.datasetCodec;
import static edu.si.trellis.IRICodec.iriCodec;
import static edu.si.trellis.InputStreamCodec.inputStreamCodec;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import edu.si.trellis.CassandraBinaryService;
import edu.si.trellis.CassandraResourceService;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.trellisldp.api.IdentifierService;

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
        ConsistencyLevel consistency = ONE;
        this.resourceService = new CassandraResourceService(new edu.si.trellis.query.rdf.Delete(session, ONE),
                        new edu.si.trellis.query.rdf.Get(session, ONE),
                        new edu.si.trellis.query.rdf.ImmutableInsert(session, consistency),
                        new edu.si.trellis.query.rdf.MutableInsert(session, consistency),
                        new edu.si.trellis.query.rdf.Mementos(session, consistency),
                        new edu.si.trellis.query.rdf.Touch(session, consistency),
                        new edu.si.trellis.query.rdf.MutableRetrieve(session, consistency),
                        new edu.si.trellis.query.rdf.ImmutableRetrieve(session, consistency),
                        new edu.si.trellis.query.rdf.BasicContainment(session, consistency));
        resourceService.initializeQueriesAndRoot();
        this.binaryService = new CassandraBinaryService((IdentifierService) null, 1024 * 1024,
                        new edu.si.trellis.query.binary.Get(session, consistency),
                        new edu.si.trellis.query.binary.Insert(session, consistency),
                        new edu.si.trellis.query.binary.Delete(session, consistency),
                        new edu.si.trellis.query.binary.Read(session, consistency),
                        new edu.si.trellis.query.binary.ReadRange(session, consistency));
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
