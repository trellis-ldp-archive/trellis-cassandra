package edu.si.trellis.cassandra;

import static javax.ws.rs.client.ClientBuilder.newClient;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.ext.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.trellisldp.test.AbstractApplicationLdpTests;

@Disabled
public class LdpIT extends AbstractApplicationLdpTests {

    private static final Logger log = getLogger(LdpIT.class);

    private static Client client = ClientBuilder.newBuilder().connectTimeout(2, TimeUnit.MINUTES).build();

    static {
        log.debug("Using JAX-RS client class: {}", client.getClass());
    }

    private static final Integer port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port;

    private volatile boolean initialized = false;

    @Override
    public synchronized Client getClient() {
        // if (!initialized) {
        // Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        // log.info("Using JAX-RS client class: {}", client.getClass());
        // try (CloseableHttpClient httpClient = HttpClients.createMinimal();
        // CloseableHttpResponse res1 = httpClient.execute(new HttpGet(trellisUri))) {
        // // if there is not root container
        // if (SC_NOT_FOUND == res1.getStatusLine().getStatusCode()) {
        // // build one
        // HttpPut req = new HttpPut(trellisUri);
        // req.setHeader("Content-Type", "text/turtle");
        // req.setHeader("Link", "<http://www.w3.org/ns/ldp#BasicContainer>; rel=\"type\"");
        // try (CloseableHttpResponse res2 = httpClient.execute(req)) {
        // assertEquals(SC_CREATED, res2.getStatusLine().getStatusCode(),
        // "Failed to create root container!");
        // initialized = true;
        // }
        // }
        // } catch (IOException e) {
        // throw new UncheckedIOException(e);
        // }
        //
        // }
        return client;
    }

    @Override
    public String getBaseURL() {
        return trellisUri;
    }

}
