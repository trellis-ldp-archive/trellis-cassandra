package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.HttpHeaders.LINK;
import static javax.ws.rs.core.Link.TYPE;
import static javax.ws.rs.core.Link.fromUri;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.trellisldp.vocabulary.LDP;

@TestInstance(PER_CLASS)
public class SimpleBinaryIT extends IT {

    private static final Logger log = getLogger(SimpleBinaryIT.class);

    @Test
    @DisplayName("Test the ability to store a binary")
    public void simpleTest() throws IOException {
        log.info("Creating container...");
        Entity<String> entity = entity("<> a <http://example.com/thing>", "text/turtle");
        Response res = getClient().target(getBaseURL()).request()
                        .header(LINK, fromUri(LDP.BasicContainer.getIRIString()).rel(TYPE).build())
                        .header("Slug", "MyContainer").post(entity);
        log.info("Headers:");
        res.getHeaders().forEach((k,v)->log.info("{}: {}", k, v));
        log.info("Status code: {}", res.getStatus());
        log.info("Status: {}", res.getStatusInfo().getReasonPhrase());
        log.info("Body: {}", IOUtils.toString((InputStream) res.getEntity(), UTF_8));
        URI container = res.getLocation();
        
        log.info("Creating binary...");
        entity = entity("some thing", TEXT_PLAIN_TYPE);
        res = getClient().target(container).request().header("Slug", "MyBinary").post(entity);
        log.info("Headers:");
        res.getHeaders().forEach((k,v)->log.info("{}: {}", k, v));
        log.info("Status code: {}", res.getStatus());
        log.info("Status: {}", res.getStatusInfo().getReasonPhrase());
        log.info("Body: {}", IOUtils.toString((InputStream) res.getEntity(), UTF_8));
        URI binary = res.getLocation();
        
        log.info("Retrieving binary...");
        res = getClient().target(binary).request().get();
        log.info("Headers:");
        res.getHeaders().forEach((k,v)->log.info("{}: {}", k, v));
        log.info("Status code: {}", res.getStatus());
        log.info("Status: {}", res.getStatusInfo().getReasonPhrase());
        log.info("Body: {}", IOUtils.toString((InputStream) res.getEntity(), UTF_8));
        String describedBy = res.getStringHeaders().get("Link").stream().filter(h -> h.endsWith("rel=\"describedby\""))
                        .findFirst().orElseThrow(RuntimeException::new);
        String u = describedBy.split(";")[0];
        String description = u.substring(1, u.length()-1);
        res = getClient().target(description).request().get();
        log.info("Headers:");
        res.getHeaders().forEach((k,v)->log.info("{}: {}", k, v));
        log.info("Status code: {}", res.getStatus());
        log.info("Status: {}", res.getStatusInfo().getReasonPhrase());
        log.info("Body: {}", IOUtils.toString((InputStream) res.getEntity(), UTF_8));
    }
}
