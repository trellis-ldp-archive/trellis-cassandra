package edu.si.trellis.cassandra;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.client.ClientBuilder.newBuilder;
import static org.slf4j.LoggerFactory.getLogger;

import javax.ws.rs.client.Client;

import org.slf4j.Logger;
import org.trellisldp.test.LdpBasicContainerTests;

public class LdpBasicContainerIT implements LdpBasicContainerTests {

    private static final Logger log = getLogger(LdpBasicContainerIT.class);

    private static Client client = newBuilder().connectTimeout(2, MINUTES).build();

    private static final String trellisUri = "http://localhost:" + getInteger("trellis.port");

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
