package edu.si.trellis.cassandra;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class CassandraResourceServiceIT extends Assert {

    private static final Logger log = LoggerFactory.getLogger(CassandraResourceServiceIT.class);

    @Test
    public void isCassandraUpForTests() {
        int port = Integer.getInteger("cassandra.nativeTransportPort");
        log.info("Using port {} for Cassandra native transport.", port);
        Builder clusterBuilder = Cluster.builder().addContactPoint("127.0.0.1").withPort(port);
        try (Cluster cluster = clusterBuilder.build(); Session session = cluster.connect()) {
            String cv = session.execute("select release_version from system.local").one().getString("release_version");
            log.info("Using Cassandra version: {}", cv);
        } catch (NoHostAvailableException e) {
            fail("Couldn't find Cassandra because: " + e.getLocalizedMessage());
        }
    }
}
