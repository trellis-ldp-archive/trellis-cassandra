package edu.si.trellis;

import static com.datastax.driver.core.TypeCodec.bigint;
import static edu.si.trellis.DatasetCodec.datasetCodec;
import static edu.si.trellis.IRICodec.iriCodec;
import static edu.si.trellis.InputStreamCodec.inputStreamCodec;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.tamaya.inject.api.Config;
import org.slf4j.Logger;

/**
 * Provides a Cassandra {@link Session} and other context for operating Cassandra-based services.
 *
 */
@ApplicationScoped
public class CassandraContext {

    private static final Logger log = getLogger(CassandraContext.class);

    @Inject
    @Config(value = { "cassandra.contactPort", "CASSANDRA_CONTACT_PORT" }, defaultValue = "9042")
    private String contactPort;

    @Inject
    @Config(value = { "cassandra.contactAddress", "CASSANDRA_CONTACT_ADDRESS" }, defaultValue = "localhost")
    private String contactAddress;

    @Inject
    @Config(value = { "cassandra.maxChunkSize", "CASSANDRA_MAX_CHUNK_SIZE" }, defaultValue = "1048576")
    private String defaultChunkSize;

    @Inject
    @Config(value = { "cassandra.binaryReadConsistency", "CASSANDRA_BINARY_READ_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel binaryReadConsistency;

    @Inject
    @Config(value = { "cassandra.binaryWriteConsistency", "CASSANDRA_BINARY_WRITE_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel binaryWriteConsistency;

    @Inject
    @Config(value = { "cassandra.rdfReadConsistency", "CASSANDRA_RDF_READ_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel rdfReadConsistency;

    @Inject
    @Config(value = { "cassandra.rdfWriteConsistency", "CASSANDRA_RDF_WRITE_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel rdfWriteConsistency;

    /**
     * @return the default size of chunk for a {@link CassandraBinaryService}
     */
    @Produces
    @DefaultChunkSize
    public int defaultChunkSize() {
        return parseInt(defaultChunkSize);
    }

    /**
     * @return the read-consistency to use querying Cassandra binary data
     */
    @Produces
    @BinaryReadConsistency
    public ConsistencyLevel binaryReadConsistency() {
        return binaryReadConsistency;
    }

    /**
     * @return the write-consistency to use querying Cassandra binary data
     */
    @Produces
    @BinaryWriteConsistency
    public ConsistencyLevel binaryWriteConsistency() {
        return binaryWriteConsistency;
    }

    /**
     * @return the read-consistency to use querying Cassandra RDF data
     */
    @Produces
    @RdfReadConsistency
    public ConsistencyLevel rdfReadConsistency() {
        return rdfReadConsistency;
    }

    /**
     * @return the write-consistency to use querying Cassandra RDF data
     */
    @Produces
    @RdfWriteConsistency
    public ConsistencyLevel rdfWriteConsistency() {
        return rdfWriteConsistency;
    }

    private Cluster cluster;

    private Session session;

    private final CountDownLatch sessionInitialized = new CountDownLatch(1);

    /**
     * Poll timeout in ms for waiting for Cassandra connection.
     */
    private static final int POLL_TIMEOUT = 1000;

    private static final TypeCodec<?>[] STANDARD_CODECS = new TypeCodec<?>[] { SimpleTimestampCodec.instance,
            inputStreamCodec, iriCodec, datasetCodec, bigint(), InstantCodec.instance };

    /**
     * Connect to Cassandra, lazily.
     */
    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        log.debug("Looking for connection...");
        this.cluster = Cluster.builder().withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                        .withoutJMXReporting().withoutMetrics().addContactPoint(contactAddress)
                        .withPort(parseInt(contactPort)).build();
        if (log.isDebugEnabled()) cluster.register(QueryLogger.builder().build());
        cluster.getConfiguration().getCodecRegistry().register(STANDARD_CODECS);
        Timer connector = new Timer("Cassandra Connection Maker", true);
        log.debug("Connecting to Cassandra...");
        connector.schedule(new TimerTask() {
            @Override
            public void run() {
                if (isPortOpen(contactAddress, contactPort)) {
                    session = cluster.connect("trellis");
                    log.debug("Connection made and keyspace set to 'trellis'.");
                    sessionInitialized.countDown();
                    this.cancel();
                    connector.cancel();
                } else log.debug("Still trying connection to {}:{}...", contactAddress, contactPort);
            }
        }, 0, POLL_TIMEOUT);
    }

    private static boolean isPortOpen(String ip, String port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, parseInt(port)), POLL_TIMEOUT);
            return true;
        } catch (@SuppressWarnings("unused") IOException e) {
            return false;
        }
    }

    /**
     * @return a {@link Session} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public Session getSession() {
        try {
            sessionInitialized.await();
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw new InterruptedStartupException("Interrupted while connectin to Cassandra!", e);
        }
        return session;
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
