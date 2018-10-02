package edu.si.trellis.cassandra;

import static com.datastax.driver.core.TypeCodec.bigint;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.tamaya.inject.api.Config;
import org.slf4j.Logger;

/**
 * Provides a Cassandra {@link Session}.
 *
 */
@ApplicationScoped
public class CassandraSession {

    private Cluster cluster;

    private Session session;

    @Inject
    @Config(value = { "cassandra.contactPort", "CASSANDRA_CONTACT_PORT" }, defaultValue = "9042")
    private String contactPort;

    @Inject
    @Config(value = { "cassandra.contactAddress", "CASSANDRA_CONTACT_ADDRESS" }, defaultValue = "localhost")
    private String contactAddress;

    private Future<Session> futureSession;

    private final ExecutorService connectionMaker = Executors.newSingleThreadExecutor();

    private static final Logger log = getLogger(CassandraSession.class);

    /**
     * Connect to Cassandra, eagerly.
     */
    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        this.cluster = Cluster.builder().withoutJMXReporting().withoutMetrics().addContactPoint(contactAddress)
                        .withPort(parseInt(contactPort)).build();
        if (log.isDebugEnabled()) cluster.register(QueryLogger.builder().build());
        cluster.getConfiguration().getCodecRegistry().register(iriCodec, datasetCodec, bigint(), InstantCodec.instance);
        this.session = cluster.connect("trellis");
        // this.futureSession = connectionMaker.submit(() -> {
        // while (true) {
        // try {
        // if (cluster != null && !cluster.isClosed()) cluster.close();
        // this.cluster = Cluster.builder().withoutJMXReporting().withoutMetrics().addContactPoint(contactNode)
        // .withPort(parseInt(contactPort)).build();
        // // this.cluster.register(QueryLogger.builder().build());
        // cluster.getConfiguration().getCodecRegistry().register(iriCodec, datasetCodec, bigint(),
        // InstantCodec.instance);
        //
        // Session session = cluster.connect("trellis");
        // return session;
        // } catch (NoHostAvailableException e) {
        // log.error("Waiting on Cassandra…", e);
        // Uninterruptibles.sleepUninterruptibly(5, SECONDS);
        // }
        // }
        // });
    }

    /**
     * @return a {@link Session} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public Session getSession() {
        return session;
        // return this.session = Futures.getUnchecked(futureSession);
    }

    /**
     * Release resources.
     */
    @PreDestroy
    public void close() {
        session.close();
        cluster.close();
    }
}
