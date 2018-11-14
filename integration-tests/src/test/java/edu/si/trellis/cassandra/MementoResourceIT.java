package edu.si.trellis.cassandra;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.trellisldp.http.core.HttpConstants.ACCEPT_DATETIME;
import static org.trellisldp.http.core.HttpConstants.MEMENTO_DATETIME;
import static org.trellisldp.test.TestUtils.getLinks;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.trellisldp.test.MementoResourceTests;

public class MementoResourceIT extends IT implements MementoResourceTests {

    @Override
    public Map<String, String> getMementos() {
        final Map<String, String> mementos = new HashMap<>();
        try (final Response res = target(getResourceLocation()).request().get()) {
            getLinks(res).stream().filter(link -> link.getRel().equals("memento"))
                .filter(l -> l.getParams().containsKey("datetime"))
                .forEach(link -> mementos.put(link.getUri().toString(), link.getParams().get("datetime")));
        }
        return mementos;
    }

    @Override
    public void testMementoDateTimeHeader() {
        getMementos().forEach((memento, date) -> {
            try (final Response res = target(memento).request().get()) {
                assertEquals(SUCCESSFUL, res.getStatusInfo().getFamily(), "Check for a successful memento request");
                final ZonedDateTime zdt = ZonedDateTime.parse(date, RFC_1123_DATE_TIME);
                assertEquals(zdt, ZonedDateTime.parse(res.getHeaderString(MEMENTO_DATETIME), RFC_1123_DATE_TIME),
                        "Check that the memento-datetime header is correct");
            }
        });
    }

    @Override
    public void testMementoAcceptDateTimeHeader() {
        getMementos().forEach((memento, date) -> {
            try (final Response res = target(getResourceLocation()).request().header(ACCEPT_DATETIME, date).get()) {
                assertEquals(SUCCESSFUL, res.getStatusInfo().getFamily(), "Check for a successful memento request");
                final ZonedDateTime zdt = ZonedDateTime.parse(date, RFC_1123_DATE_TIME);
                assertEquals(zdt, ZonedDateTime.parse(res.getHeaderString(MEMENTO_DATETIME), RFC_1123_DATE_TIME),
                        "Check that the memento-datetime header is correct");
            }
        });
    }}
