package edu.si.trellis.cassandra;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static javax.ws.rs.core.Response.Status.Family.REDIRECTION;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.trellisldp.http.core.HttpConstants.ACCEPT_DATETIME;
import static org.trellisldp.http.core.HttpConstants.MEMENTO_DATETIME;

import java.net.URI;
import java.time.ZonedDateTime;

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.test.MementoBinaryTests;

public class MementoBinaryIT extends IT implements MementoBinaryTests {
    static Logger log = LoggerFactory.getLogger(MementoBinaryIT.class);

    @Override
    @Test
    public void testMementoAcceptDateTimeHeader() {
        getMementos().forEach((memento, date) -> {
            log.info("Using resource location: {}", getResourceLocation());
            log.info("Using date: {}", date);
            try (final Response res1 = target(getResourceLocation()).request().header(ACCEPT_DATETIME, date).head()) {
                log.info("Got headers for initial request:");
                res1.getHeaders().forEach((k, v) -> log.info("{} : {}", k, v));
                assertEquals(REDIRECTION, res1.getStatusInfo().getFamily(), "Check for a redirection to the memento");
                URI locationUri = res1.getLocation();
                String location = locationUri == null ? "None" : locationUri.toString();
                log.info("Got location for second request: {}", location);
                try (final Response res2 = target(location).request().header(ACCEPT_DATETIME, date).head()) {
                    log.info("Got response status code: {}", res2.getStatus());
                    log.info("Got headers for second request:");
                    res2.getHeaders().forEach((k, v) -> log.info("{} : {}", k, v));
                    assertEquals(SUCCESSFUL, res2.getStatusInfo().getFamily(),
                                    "Check for a successful memento request");
                    final ZonedDateTime zdt = ZonedDateTime.parse(date, RFC_1123_DATE_TIME);
                    assertEquals(zdt, ZonedDateTime.parse(res2.getHeaderString(MEMENTO_DATETIME), RFC_1123_DATE_TIME),
                                    "Check that the memento-datetime header is correct");
                }
            }
        });
    }

}
