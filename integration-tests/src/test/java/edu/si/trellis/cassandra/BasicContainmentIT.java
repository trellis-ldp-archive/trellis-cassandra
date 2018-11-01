package edu.si.trellis.cassandra;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.client.ClientBuilder.newBuilder;
import static org.slf4j.LoggerFactory.getLogger;

import javax.ws.rs.client.Client;

import org.slf4j.Logger;
import org.trellisldp.test.LdpBasicContainerTests;

public class BasicContainmentIT implements LdpBasicContainerTests {

    private static final Logger log = getLogger(BasicContainmentIT.class);

    private static Client client = newBuilder().connectTimeout(2, MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

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
//
//    @Override
//    public void testGetEmptyContainer() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testGetEmptyContainer();
//    }
//
//    @Override
//    public void testGetInverseEmptyContainer() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testGetInverseEmptyContainer();
//    }
//
//    @Override
//    public void testGetContainer() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testGetContainer();
//    }
//
//    @Override
//    public void testCreateContainerViaPut() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testCreateContainerViaPut();
//    }
//
//    @Override
//    public void testCreateContainerWithSlug() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testCreateContainerWithSlug();
//    }
//
//    @Override
//    public void testDeleteContainer() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testDeleteContainer();
//    }
}
