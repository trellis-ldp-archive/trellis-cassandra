package edu.si.trellis;

import static java.util.UUID.randomUUID;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.UUID;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class BasicOperationIT {

    private static final Logger log = getLogger(BasicOperationIT.class);

    private static final CloseableHttpClient client = HttpClients.createMinimal();

    private static final Integer port = Integer.getInteger("trellis.port");

    private static final String trellisUri = "http://localhost:" + port + "/";

    @Test
    public void smokeTest() throws MalformedURLException, IOException {
        UUID slug = randomUUID();
        String id;
        log.info("Using Slug {} to smoke test webapp.", slug);
        HttpPost req = new HttpPost(trellisUri);
        req.setHeader("Slug", slug.toString());
        req.setHeader("Content-Type", "text/turtle");
        req.setEntity(new StringEntity("<> a <http://example.com/example> ."));
        try (CloseableHttpResponse res = client.execute(req); InputStream url = res.getEntity().getContent()) {
            assertEquals(SC_CREATED, res.getStatusLine().getStatusCode());
            id = res.getFirstHeader("Location").getValue();
        }
        log.info("Using location {} for resource location.", id);
        HttpGet get = new HttpGet(id);
        try (CloseableHttpResponse res = client.execute(get); InputStream url = res.getEntity().getContent()) {
            assertEquals(SC_OK, res.getStatusLine().getStatusCode());
            String responseBody = EntityUtils.toString(res.getEntity());
            log.debug("Response body for resource: {}", responseBody);
            assertTrue(responseBody.contains("http://example.com/example"));
        }
    }
}
