package edu.si.trellis.cassandra;

import static com.datastax.driver.core.TypeCodec.bigint;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.tamaya.inject.api.Config;
import org.slf4j.Logger;
import org.trellisldp.api.RuntimeTrellisException;

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
    private final CountDownLatch sessionInitialized = new CountDownLatch(1);

    private static final Logger log = getLogger(CassandraSession.class);

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
        this.cluster = Cluster.builder().withoutJMXReporting().withoutMetrics().addContactPoint(contactAddress)
                        .withPort(parseInt(contactPort)).build();
        if (log.isDebugEnabled()) cluster.register(QueryLogger.builder().build());
        cluster.getConfiguration().getCodecRegistry().register(iriCodec, datasetCodec, bigint(), InstantCodec.instance);
        Timer connector = new Timer("Cassandra Connection Maker");
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (portIsOpen(contactAddress, contactPort)) {
                    session = cluster.connect("trellis");
                    log.debug("Set keyspace to trellis");
                    cancel();
                    sessionInitialized.countDown();
                }
            }
        };
        connector.schedule(task, 0, POLL_TIMEOUT);

    }

    private static boolean portIsOpen(String ip, String port) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(ip, parseInt(port)), POLL_TIMEOUT);
            socket.close();
            return true;
        } catch (IOException e) {
            log.debug("Waiting for connection to {}:{}", ip, port);
            return false;
        }
    }

    /**
     * @return a {@link Session} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public Session getSession() {
        awaitUninterruptibly(sessionInitialized);
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
