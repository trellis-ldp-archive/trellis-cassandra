package edu.si.trellis;

import static edu.si.trellis.DatasetCodec.datasetCodec;
import static edu.si.trellis.IRICodec.iriCodec;
import static edu.si.trellis.InputStreamCodec.inputStreamCodec;
import static java.lang.Integer.parseInt;
import static java.net.InetSocketAddress.createUnresolved;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

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
    private ConsistencyLevel binaryReadConsistency;

    @Inject
    @Config(key = "cassandra.binaryWriteConsistency", alternateKeys = {
            "CASSANDRA_BINARY_WRITE_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel binaryWriteConsistency;

    @Inject
    @Config(key = "cassandra.rdfReadConsistency", alternateKeys = {
            "CASSANDRA_RDF_READ_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel rdfReadConsistency;

    @Inject
    @Config(key = "cassandra.rdfWriteConsistency", alternateKeys = {
            "CASSANDRA_RDF_WRITE_CONSISTENCY" }, defaultValue = "ONE")
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
    @MutableReadConsistency
    public ConsistencyLevel rdfReadConsistency() {
        return rdfReadConsistency;
    }

    /**
     * @return the write-consistency to use querying Cassandra RDF data
     */
    @Produces
    @MutableWriteConsistency
    public ConsistencyLevel rdfWriteConsistency() {
        return rdfWriteConsistency;
    }

    private CompletionStage<CqlSession> futureSession;

    private CqlSession session;

    private static final TypeCodec<?>[] STANDARD_CODECS = new TypeCodec<?>[] { inputStreamCodec, iriCodec,
            datasetCodec };

    /**
     * Connect to Cassandra, lazily.
     */
    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        log.debug("Looking for connection...");
        final InetSocketAddress socketAddress = createUnresolved(contactAddress, parseInt(contactPort));

        this.futureSession = CqlSession.builder()
                        .addTypeCodecs(STANDARD_CODECS)
                        .withKeyspace("trellis")
                        .withLocalDatacenter("datacenter1")
                        .addContactPoint(socketAddress).buildAsync();
    }

    /**
     * @return a {@link CqlSession} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public synchronized CqlSession getSession() {
        return session == null ? session = futureSession.toCompletableFuture().join() : session;
    }

    /**
     * Release resources.
     */
    @PreDestroy
    public void close() {
        session.close();
    }
}
