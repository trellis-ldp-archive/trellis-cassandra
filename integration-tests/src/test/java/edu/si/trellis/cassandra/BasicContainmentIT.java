package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.EntityTag;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.trellisldp.test.CommonTests;
import org.trellisldp.test.LdpBasicContainerTests;

public class BasicContainmentIT implements CommonTests, LdpBasicContainerTests {

    private static final Logger log = getLogger(BasicContainmentIT.class);

    private static Client client = ClientBuilder.newBuilder().connectTimeout(2, TimeUnit.MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

    private static final Integer port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port;

    private String child, container;
    private EntityTag etag1, etag2, etag3, etag4;

    @Override
    public void testGetEmptyContainer() {
        // TODO Auto-generated method stub
        LdpBasicContainerTests.super.testGetEmptyContainer();
    }

    @Override
    public void testGetInverseEmptyContainer() {
        // TODO Auto-generated method stub
        LdpBasicContainerTests.super.testGetInverseEmptyContainer();
    }

    @Override
    public void testGetContainer() {
        // TODO Auto-generated method stub
        LdpBasicContainerTests.super.testGetContainer();
    }

    @Override
    @Test
    public void testCreateContainerViaPost() {
        log.info("Here is testCreateContainerViaPost");
        try {  LdpBasicContainerTests.super.testCreateContainerViaPost(); }
        catch (AssertionFailedError e ) {
            log.info("That was testCreateContainerViaPost");
            throw e;
        }
        log.info("testCreateContainerViaPost succeeded!");
    }

    @Override
    public void testCreateContainerViaPut() {
        // TODO Auto-generated method stub
        LdpBasicContainerTests.super.testCreateContainerViaPut();
    }

    @Override
    public void testCreateContainerWithSlug() {
        // TODO Auto-generated method stub
        LdpBasicContainerTests.super.testCreateContainerWithSlug();
    }

    @Override
    public void testDeleteContainer() {
        // TODO Auto-generated method stub
        LdpBasicContainerTests.super.testDeleteContainer();
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public String getBaseURL() {
        return trellisUri;
    }

    @Override
    public void setChildLocation(final String location) {
        child = location;
    }

    @Override
    public String getChildLocation() {
        return child;
    }

    @Override
    public void setContainerLocation(final String location) {
        container = location;
    }

    @Override
    public String getContainerLocation() {
        return container;
    }

    @Override
    public void setFirstETag(final EntityTag etag) {
        etag1 = etag;
    }

    @Override
    public EntityTag getFirstETag() {
        return etag1;
    }

    @Override
    public void setSecondETag(final EntityTag etag) {
        etag2 = etag;
    }

    @Override
    public EntityTag getSecondETag() {
        return etag2;
    }

    @Override
    public void setThirdETag(final EntityTag etag) {
        etag3 = etag;
    }

    @Override
    public EntityTag getThirdETag() {
        return etag3;
    }

    @Override
    public void setFourthETag(final EntityTag etag) {
        etag4 = etag;
    }

    @Override
    public EntityTag getFourthETag() {
        return etag4;
    }

}
