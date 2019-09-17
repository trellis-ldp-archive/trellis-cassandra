package edu.si.trellis;

import static edu.si.trellis.DatasetCodec.DATASET_CODEC;
import static edu.si.trellis.IRICodec.IRI_CODEC;
import static edu.si.trellis.InputStreamCodec.INPUTSTREAM_CODEC;
import static java.lang.Integer.parseInt;
import static java.lang.Thread.currentThread;
import static java.net.InetSocketAddress.createUnresolved;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.tamaya.inject.api.Config;
import org.slf4j.Logger;

/**
 * Provides a Cassandra {@link CqlSession} and other context for operating Cassandra-based services.
 *
 */
@ApplicationScoped
public class CassandraContext {

    private static final Logger log = getLogger(CassandraContext.class);

    @Inject
    @Config(key = "cassandra.contactPort", alternateKeys = { "CASSANDRA_CONTACT_PORT" }, defaultValue = "9042")
    private String contactPort;

    @Inject
    @Config(key = "cassandra.contactAddress", alternateKeys = {
            "CASSANDRA_CONTACT_ADDRESS" }, defaultValue = "localhost")
    private String contactAddress;

    @Inject
    @Config(key = "cassandra.maxChunkSize", alternateKeys = {
            "CASSANDRA_MAX_CHUNK_SIZE" }, defaultValue = DefaultChunkSize.value)
    private String defaultChunkSize;

    @Inject
    @Config(key = "cassandra.binaryReadConsistency", alternateKeys = {
            "CASSANDRA_BINARY_READ_CONSISTENCY" }, defaultValue = "ONE")
    private DefaultConsistencyLevel binaryReadConsistency;

    @Inject
    @Config(key = "cassandra.binaryWriteConsistency", alternateKeys = {
            "CASSANDRA_BINARY_WRITE_CONSISTENCY" }, defaultValue = "ONE")
    private DefaultConsistencyLevel binaryWriteConsistency;

    @Inject
    @Config(key = "cassandra.rdfReadConsistency", alternateKeys = {
            "CASSANDRA_RDF_READ_CONSISTENCY" }, defaultValue = "ONE")
    private DefaultConsistencyLevel rdfReadConsistency;

    @Inject
    @Config(key = "cassandra.rdfWriteConsistency", alternateKeys = {
            "CASSANDRA_RDF_WRITE_CONSISTENCY" }, defaultValue = "ONE")
    private DefaultConsistencyLevel rdfWriteConsistency;

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
    public DefaultConsistencyLevel binaryReadConsistency() {
        return binaryReadConsistency;
    }

    /**
     * @return the write-consistency to use querying Cassandra binary data
     */
    @Produces
    @BinaryWriteConsistency
    public DefaultConsistencyLevel binaryWriteConsistency() {
        return binaryWriteConsistency;
    }

    /**
     * @return the read-consistency to use querying Cassandra RDF data
     */
    @Produces
    @MutableReadConsistency
    public DefaultConsistencyLevel rdfReadConsistency() {
        return rdfReadConsistency;
    }

    /**
     * @return the write-consistency to use querying Cassandra RDF data
     */
    @Produces
    @MutableWriteConsistency
    public DefaultConsistencyLevel rdfWriteConsistency() {
        return rdfWriteConsistency;
    }

    private CountDownLatch sessionGuard = new CountDownLatch(1);

    private CqlSession session;

    private static final TypeCodec<?>[] STANDARD_CODECS = new TypeCodec<?>[] { INPUTSTREAM_CODEC, IRI_CODEC,
            DATASET_CODEC };

    /**
     * Connect to Cassandra, lazily.
     */
    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        log.debug("Looking for connection...");
        final InetSocketAddress socketAddress = createUnresolved(contactAddress, parseInt(contactPort));

        CompletionStage<CqlSession> futureSession = CqlSession.builder()
                        .addTypeCodecs(STANDARD_CODECS)
                        .withKeyspace("trellis")
                        .withLocalDatacenter("datacenter1")
                        .addContactPoint(socketAddress)
                        .buildAsync();
        futureSession.thenApply(Objects::requireNonNull)
                        .thenAccept(this::session)
                        .thenRun(sessionGuard::countDown)
                        .thenRun(()->log.debug("Connection found."));
    }

    private void session(CqlSession s) {
        this.session = s;
    }

    /**
     * @return a {@link CqlSession} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public CqlSession session() {
        boolean interrupted = false;
        try {
            while (true)
                try {
                    sessionGuard.await();
                    return session;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
        } finally {
            if (interrupted) currentThread().interrupt();
        }
    }

    /**
     * Release resources.
     */
    @PreDestroy
    public void close() {
        session.close();
    }
}
