package edu.si.trellis.cassandra;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.client.ClientBuilder.newBuilder;
import static org.slf4j.LoggerFactory.getLogger;

import javax.ws.rs.client.Client;

import org.slf4j.Logger;
import org.trellisldp.test.LdpBinaryTests;

public class LdpBinaryIT implements LdpBinaryTests {

    private static final Logger log = getLogger(LdpBinaryIT.class);

    private static Client client = newBuilder().connectTimeout(2, MINUTES).build();

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
}
