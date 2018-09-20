package edu.si.trellis.cassandra;

import static com.datastax.driver.core.TypeCodec.bigint;
import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static edu.si.trellis.cassandra.IRICodec.iriCodec;
import static java.lang.Integer.parseInt;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;

import java.io.Closeable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.tamaya.inject.api.Config;
import org.slf4j.Logger;

@ApplicationScoped
public class CassandraSession implements Closeable {

    private Cluster cluster;

    private Session session;

    @Inject
    @Config
    private String contactPort;

    @Inject
    @Config
    private String contactAddress;

    private static final Logger log = getLogger(CassandraSession.class);

    @PostConstruct
    public void connect() {
        log.info("Using Cassandra node address: {} and port: {}", contactAddress, contactPort);
        this.cluster = Cluster.builder().withoutJMXReporting().withoutMetrics().addContactPoint(contactAddress)
                        .withPort(parseInt(contactPort)).build();
        // this.cluster.register(QueryLogger.builder().build());
        cluster.getConfiguration().getCodecRegistry().register(iriCodec, datasetCodec, bigint(), InstantCodec.instance);
        this.session = cluster.connect("Trellis");
    }

    /**
     * @return a {@link Session} for use with {@link CassandraResourceService} (and {@link CassandraBinaryService})
     */
    @Produces
    @ApplicationScoped
    public Session getSession() {
        return session;
    }

    @PreDestroy
    @Override
    public void close() {
        session.close();
        cluster.close();
    }
}
