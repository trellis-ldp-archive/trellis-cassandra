package edu.si.trellis.cassandra;

import static org.apache.jena.rdf.model.ModelFactory.createModelForGraph;
import static org.apache.jena.sparql.sse.SSE.parseGraph;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class GraphCodecTest extends Assert {

    private static final JenaRDF rdf = new JenaRDF();

    Logger log = LoggerFactory.getLogger(GraphCodecTest.class);

    @Test(expected = InvalidTypeException.class)
    public void badParse() {
        DatasetCodec graphCodec = new DatasetCodec(DataType.text());
        String badTriples = "dfshgou;sdfhgoudfhogh";
        graphCodec.parse(badTriples);
    }

    @Test
    public void testParse() {
        DatasetCodec graphCodec = new DatasetCodec(DataType.text());
        String triple = "<s> <p> <o> .";
        //Graph graph = graphCodec.parse(triple);
        //Triple found = graph.stream().findFirst().orElseThrow(AssertionError::new);
        //assertEquals("Found wrong triple!", triple, found.toString());
    }

    @Test
    public void testDeserialize() {
        DatasetCodec graphCodec = new DatasetCodec(DataType.text());
        String triple = "<s> <p> <o> .";
        ByteBuffer bytes = ByteBuffer.wrap(triple.getBytes());
        // graph = graphCodec.deserialize(bytes, null);
        //Triple found = graph.stream().findFirst().orElseThrow(AssertionError::new);
       //("Found wrong triple!", triple, found.toString());
    }

    @Test
    public void testSerialize() {
        DatasetCodec graphCodec = new DatasetCodec(DataType.text());
        String triple = "<s> <p> <o>";
        Graph graph = rdf.asGraph(createModelForGraph(parseGraph("( graph ( triple " + triple + "))")));
        //ByteBuffer bytes = graphCodec.serialize(graph, null);
        // we add the period below because the triple is now in a graph
       // assertEquals("Found wrong triple!", triple + " .\n", new String(bytes.array()));
    }
}
