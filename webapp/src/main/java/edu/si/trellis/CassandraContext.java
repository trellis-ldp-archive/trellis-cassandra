package edu.si.trellis;

import static com.datastax.driver.core.TypeCodec.bigint;
import static edu.si.trellis.DatasetCodec.datasetCodec;
import static edu.si.trellis.IRICodec.iriCodec;
import static edu.si.trellis.InputStreamCodec.inputStreamCodec;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

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
    @Config(key = "cassandra.contactPort", alternateKeys = { "CASSANDRA_CONTACT_PORT" }, defaultValue = "9042")
    private String contactPort;

    @Inject
    @Config(key = "cassandra.contactAddress", alternateKeys = { "CASSANDRA_CONTACT_ADDRESS" }, defaultValue = "localhost")
    private String contactAddress;

    @Inject
    @Config(key = "cassandra.maxChunkSize", alternateKeys = {
            "CASSANDRA_MAX_CHUNK_SIZE" }, defaultValue = DefaultChunkSize.value)
    private String defaultChunkSize;

    @Inject
    @Config(key = "cassandra.binaryReadConsistency", alternateKeys = { "CASSANDRA_BINARY_READ_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel binaryReadConsistency;

    @Inject
    @Config(key = "cassandra.binaryWriteConsistency", alternateKeys = { "CASSANDRA_BINARY_WRITE_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel binaryWriteConsistency;

    @Inject
    @Config(key = "cassandra.rdfReadConsistency", alternateKeys = { "CASSANDRA_RDF_READ_CONSISTENCY" }, defaultValue = "ONE")
    private ConsistencyLevel rdfReadConsistency;

    @Inject
    @Config(key = "cassandra.rdfWriteConsistency", alternateKeys = { "CASSANDRA_RDF_WRITE_CONSISTENCY" }, defaultValue = "ONE")
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

    private Cluster cluster;

    private Session session;

    private static final TypeCodec<?>[] STANDARD_CODECS = new TypeCodec<?>[] { SimpleTimestampCodec.instance,
            inputStreamCodec, iriCodec, datasetCodec, bigint(), InstantCodec.instance };

    /**
     * Connect to Cassandra.
     */
    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        this.cluster = Cluster.builder().withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                        .withoutJMXReporting().withoutMetrics().addContactPoint(contactAddress)
                        .withPort(parseInt(contactPort)).build();
        if (log.isDebugEnabled()) cluster.register(QueryLogger.builder().withMaxParameterValueLength(1000).build());
        cluster.getConfiguration().getCodecRegistry().register(STANDARD_CODECS);
        log.info("Connecting to Cassandra...");
        this.session = cluster.connect("Trellis");
        log.info("Connected to Cassandra.");
    }

    /**
     * @return a {@link Session} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public Session getSession() {
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
