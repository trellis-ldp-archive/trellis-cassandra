package edu.si.trellis.cassandra;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.client.ClientBuilder.newBuilder;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collections;
import java.util.Set;

import javax.ws.rs.client.Client;

import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.trellisldp.test.LdpRdfTests;

@Disabled
public class LdpRdfIT implements LdpRdfTests {

    private static final Logger log = getLogger(LdpRdfIT.class);

    private static Client client = newBuilder().connectTimeout(2, MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

    private static final String trellisUri = "http://localhost:" + getInteger("trellis.port");

    @Override
    public synchronized Client getClient() {
        return client;
    }

    @Override
    public String getBaseURL() {
        return trellisUri;
    }

    private String resourceLocation;
    
    @Override
    public void setResourceLocation(String location) {
        this.resourceLocation = location;
    }

    @Override
    public String getResourceLocation() {
        return resourceLocation;
    }

    @Override
    public Set<String> supportedJsonLdProfiles() {
        return Collections.emptySet();
    }
}
