package edu.si.trellis.cassandra;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.client.ClientBuilder.newBuilder;
import static org.apache.commons.rdf.api.RDFSyntax.TURTLE;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.TrellisUtils.getInstance;
import static org.trellisldp.http.core.RdfMediaType.TEXT_TURTLE_TYPE;
import static org.trellisldp.test.TestUtils.readEntityAsGraph;

import java.util.Collections;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.RDF;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.trellisldp.test.LdpRdfTests;
import org.trellisldp.vocabulary.LDP;

//@Disabled
public class LdpRdfIT implements LdpRdfTests {

    private static Client client = newBuilder().connectTimeout(2, MINUTES).build();

    private static final String trellisUri = "http://localhost:" + getInteger("trellis.port") + "/";

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
