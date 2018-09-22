package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.trellisldp.test.AbstractApplicationLdpTests;

public class ApplicationLdpIT extends AbstractApplicationLdpTests {

    private static final Logger log = getLogger(ApplicationLdpIT.class);

    private Client client;

    private static final Integer port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port + "/webapp/";

    @BeforeEach
    public void beforeTest() {
        this.client = ClientBuilder.newClient();
        if (client ==null) throw new AssertionError("bad client!!!");
        log.info("Using JAX-RS client class: {}", client.getClass());
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public String getBaseURL() {
        return trellisUri;
    }

}
