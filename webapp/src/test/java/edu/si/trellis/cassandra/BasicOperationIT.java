package edu.si.trellis.cassandra;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.wildfly.swarm.arquillian.DefaultDeployment;


@RunWith(Arquillian.class)
@DefaultDeployment
public class BasicOperationIT {

    private static final Logger log = getLogger(BasicOperationIT.class);

    private static final CloseableHttpClient client = HttpClients.createMinimal();

    private static final Integer port = Integer.parseInt(System.getProperty("trellis.port"));

    private static final String trellisUri = "http://localhost:" + port + "/trellis-cassandra-app/";

    @Test
    public void test() throws MalformedURLException, IOException {
        String id = trellisUri + "example";
        log.info("Using IRI {}", id);
        HttpPut req = new HttpPut(id);
        req.setHeader("Content-Type", "text/turtle");
        req.setEntity(new StringEntity("<> a <http://example.com/example> ."));
        try (CloseableHttpResponse res = client.execute(req); InputStream url = res.getEntity().getContent()) {
            println();
            res.getEntity().writeTo(System.out);
            println();
            // waitformax();
            assertEquals(SC_CREATED, res.getStatusLine().getStatusCode());
        }
        HttpGet get = new HttpGet(id);
        try (CloseableHttpResponse res = client.execute(get); InputStream url = res.getEntity().getContent()) {
            assertEquals(SC_OK, res.getStatusLine().getStatusCode());
            assertTrue(EntityUtils.toString(res.getEntity()).contains("http://example.com/example"));
        }

    }

    private void waitformax() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void println() {
        log.info("-------------");
    }

}
