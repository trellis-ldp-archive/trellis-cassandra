package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.Triple;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class GraphCodecTest extends Assert {

    Logger log = LoggerFactory.getLogger(GraphCodecTest.class);

    @Test(expected = InvalidTypeException.class)
    public void badQuads() {
        GraphCodec graphCodec = new GraphCodec(DataType.text());
        String badTriples = "dfshgou;sdfhgoudfhogh";
        graphCodec.parse(badTriples);
    }

    @Test
    public void testParse() {
        GraphCodec graphCodec = new GraphCodec(DataType.text());
        String triple = "<s> <p> <o> .";
        Graph graph = graphCodec.parse(triple);
        Triple found = graph.stream().findFirst().orElseThrow(() -> new AssertionError("Didn't find the quad!"));
        assertEquals("Found wrong quad!", triple, found.toString());
    }
}
