package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.EntityTag;

import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.trellisldp.test.LdpRdfTests;

@Disabled
public class LdpRdfIT implements LdpRdfTests {

    private static final Logger log = getLogger(LdpRdfIT.class);

    private static Client client = ClientBuilder.newBuilder().connectTimeout(2, TimeUnit.MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

    private static final int port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port;

    @Override
    public synchronized Client getClient() {
        return client;
    }

    @Override
    public String getBaseURL() {
        return trellisUri;
    }

    private String resourceLocation, annotationLocation, containerLocation;
    
    private EntityTag etag1, etag2;
    
    @Override
    public void setResourceLocation(String location) {
        this.resourceLocation = location;
    }

    @Override
    public String getResourceLocation() {
        return resourceLocation;
    }

    @Override
    public void setAnnotationLocation(String location) {
        this.annotationLocation = location;
    }

    @Override
    public String getAnnotationLocation() {
        return annotationLocation;
    }

    @Override
    public void setContainerLocation(String location) {
        this.containerLocation = location;
    }

    @Override
    public String getContainerLocation() {
        return containerLocation;
    }

    @Override
    public EntityTag getFirstETag() {
        return etag1;
    }

    @Override
    public EntityTag getSecondETag() {
        return etag2;
    }

    @Override
    public void setFirstETag(EntityTag etag) {
        this.etag1 = etag;
    }

    @Override
    public void setSecondETag(EntityTag etag) {
        this.etag2 = etag;
    }

    @Override
    public Set<String> supportedJsonLdProfiles() {
        return Collections.emptySet();
    }
}
