package edu.si.trellis.cassandra;

import static com.datastax.driver.core.TypeCodec.bigint;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static edu.si.trellis.cassandra.InputStreamCodec.inputStreamCodec;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import edu.si.trellis.cassandra.CassandraBinaryService.MaxChunkSize;

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
    @Config(value = { "cassandra.maxChunkSize", "CASSANDRA_MAX_CHUNK_SIZE" }, defaultValue = "1048576")
    private String maxChunkSize;

    /**
     * @return the maximum size of chunk for a {@link CassandraBinaryService}
     */
    @Produces
    @MaxChunkSize
    public int maxChunkSize() {
        return parseInt(maxChunkSize);
    }

    private Cluster cluster;

    private Session session;

    @Inject
    @Config(value = { "cassandra.contactPort", "CASSANDRA_CONTACT_PORT" }, defaultValue = "9042")
    private String contactPort;

    @Inject
    @Config(value = { "cassandra.contactAddress", "CASSANDRA_CONTACT_ADDRESS" }, defaultValue = "localhost")
    private String contactAddress;
    private final CountDownLatch sessionInitialized = new CountDownLatch(1);

    /**
     * Poll timeout in ms for waiting for Cassandra connection.
     */
    private static final int POLL_TIMEOUT = 1000;

    /**
     * Connect to Cassandra, lazily.
     */
    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        log.debug("Looking for connection...");
        this.cluster = Cluster.builder().withoutJMXReporting().withoutMetrics().addContactPoint(contactAddress)
                        .withPort(parseInt(contactPort)).build();
        if (log.isDebugEnabled()) cluster.register(QueryLogger.builder().build());
        cluster.getConfiguration().getCodecRegistry().register(inputStreamCodec, iriCodec, datasetCodec, bigint(),
                        InstantCodec.instance);
        Timer connector = new Timer("Cassandra Connection Maker");
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (portIsOpen(contactAddress, contactPort)) {
                    session = cluster.connect("trellis");
                    log.debug("Connection made and keyspace set to 'trellis'.");
                    sessionInitialized.countDown();
                    cancel();
                }
            }
        };
        log.debug("Waiting for connection...");
        connector.schedule(task, 0, POLL_TIMEOUT);
    }

    private static boolean portIsOpen(String ip, String port) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(ip, parseInt(port)), POLL_TIMEOUT);
            socket.close();
            return true;
        } catch (IOException e) {
            log.debug("Still looking for connection to {}:{}...", ip, port);
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
            throw new RuntimeException(e);
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
