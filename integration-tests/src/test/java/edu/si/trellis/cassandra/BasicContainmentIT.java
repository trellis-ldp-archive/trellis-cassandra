package edu.si.trellis.cassandra;

import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.Response.Status.Family.CLIENT_ERROR;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static org.apache.commons.rdf.api.RDFSyntax.TURTLE;
import static org.junit.jupiter.api.Assertions.*;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.RDFUtils.getInstance;
import static org.trellisldp.http.core.RdfMediaType.TEXT_TURTLE;
import static org.trellisldp.http.core.RdfMediaType.TEXT_TURTLE_TYPE;
import static org.trellisldp.test.TestUtils.meanwhile;
import static org.trellisldp.test.TestUtils.readEntityAsGraph;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.Response;

import org.apache.commons.rdf.api.*;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.trellisldp.test.CommonTests;
import org.trellisldp.test.LdpBasicContainerTests;
import org.trellisldp.vocabulary.LDP;

public class BasicContainmentIT implements LdpBasicContainerTests {

    private static final Logger log = getLogger(BasicContainmentIT.class);

    private static Client client = ClientBuilder.newBuilder().connectTimeout(2, TimeUnit.MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

    private static final int port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port;

    private String child, container;
    private EntityTag etag1, etag2, etag3, etag4;

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
//    @Test
//    public void testDeleteContainer() {
//        final RDF rdf = getInstance();
//        final EntityTag etag;
//
//        // TODO remove the getChildLocation and setChildLocation methods from this interface
//        final String childResource;
//
//        try (final Response res = target(getContainerLocation()).request().post(entity("", TEXT_TURTLE))) {
//            assertAll("Check for an LDP-RS", checkRdfResponse(res, LDP.RDFSource, null));
//            childResource = res.getLocation().toString();
//        }
//
//        meanwhile();
//
//        try (final Response res = target(getContainerLocation()).request().get()) {
//            assertAll("Check for an LDP-BC", checkRdfResponse(res, LDP.BasicContainer, TEXT_TURTLE_TYPE));
//            final IRI identifier = rdf.createIRI(getContainerLocation());
//            final Graph g = readEntityAsGraph(res.getEntity(), getBaseURL(), TURTLE);
//            assertAll("Verify the resulting graph", checkRdfGraph(g, identifier));
//            assertTrue(g.contains(identifier, LDP.contains, rdf.createIRI(childResource)),
//                    "Check for the presence of an ldp:contains triple");
//            etag = res.getEntityTag();
//            assertTrue(etag.isWeak(), "Verify that the ETag is weak");
//        }
//
//        // Delete one of the child resources
//        try (final Response res = target(childResource).request().delete()) {
//            assertEquals(SUCCESSFUL, res.getStatusInfo().getFamily(), "Check the response type");
//        }
//        meanwhile();
//        log.info("Trying to fetch deleted resource");
//        // Try fetching the deleted resource
//        try (final Response res = target(childResource).request().get()) {
//            log.info("response: {}", res.readEntity(String.class));
//            log.info("status: {}", res.getStatusInfo().getStatusCode());
//            
//            log.info("reason: {}", res.getStatusInfo().getReasonPhrase());
//            
//            assertEquals(CLIENT_ERROR, res.getStatusInfo().getFamily(), "Check for an expected error");
//        }
//
//        try (final Response res = target(getContainerLocation()).request().get()) {
//            assertAll("Check the parent container", checkRdfResponse(res, LDP.BasicContainer, TEXT_TURTLE_TYPE));
//            final Graph g = readEntityAsGraph(res.getEntity(), getBaseURL(), TURTLE);
//            assertFalse(g.contains(rdf.createIRI(getContainerLocation()), LDP.contains,
//                        rdf.createIRI(childResource)), "Check the graph doesn't contain the deleted resource");
//            assertTrue(res.getEntityTag().isWeak(), "Check that the ETag is weak");
//            assertNotEquals(etag, res.getEntityTag(), "Verify that the ETag value is different");
//        }  
//    }
    
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
//    public void testCreateContainerViaPost() {
//        // TODO Auto-generated method stub
//        LdpBasicContainerTests.super.testCreateContainerViaPost();
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
}
