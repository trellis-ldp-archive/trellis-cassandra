package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.slf4j.Logger;
import org.trellisldp.test.LdpBasicContainerTests;

public class BasicContainmentIT implements LdpBasicContainerTests {

    private static final Logger log = getLogger(BasicContainmentIT.class);

    private static Client client = ClientBuilder.newBuilder().connectTimeout(2, TimeUnit.MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

    private static final int port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port;

    private String container;

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public String getBaseURL() {
        return trellisUri;
    }

    @Override
    public void setContainerLocation(final String location) {
        log.debug("Container location set to: {}", location);
        container = location;
    }

    @Override
    public String getContainerLocation() {
        log.debug("Container location is: {}", container);
        return container;
    }
}
