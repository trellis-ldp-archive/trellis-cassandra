package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.tamaya.inject.api.Config;
import org.slf4j.Logger;
import org.trellisldp.http.TrellisHttpResource;

/**
 * Basic JAX-RS {@link Application} to deploy Trellis with a Cassandra persistence implementation.
 *
 */
@ApplicationPath("/")
@ApplicationScoped
public class CassandraApplication extends Application {

    private static final Logger log = getLogger(CassandraApplication.class);

    @Inject
    private CassandraServiceBundler services;

    @Override
    public Set<Object> getSingletons() {
        return ImmutableSet.of(new TrellisHttpResource(services));
    }
}
