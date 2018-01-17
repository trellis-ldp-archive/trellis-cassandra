package edu.si.trellis.cassandra;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidTypeException;

public class DatasetCodecTest extends Assert {

    @Test(expected = InvalidTypeException.class)
    public void badParse() {
        DatasetCodec datasetCodec = new DatasetCodec();
        String badRDF = "dfshgou;sdfhgoudfhogh";
        datasetCodec.parse(badRDF);
    }

    @Test
    public void testParse() {
        DatasetCodec datasetCodec = new DatasetCodec();
        String quad = "<s> <p> <o> <g>.";
        Dataset dataset = datasetCodec.parse(quad);
        Quad found = dataset.stream().findFirst().orElseThrow(AssertionError::new);
        assertEquals("Found wrong quad!", quad, found.toString());
    }

    @Test
    public void testDeserialize() {
        DatasetCodec datasetCodec = new DatasetCodec();
        String quad = "<s> <p> <o> <g>.";
        ByteBuffer bytes = ByteBuffer.wrap(quad.getBytes());
        Dataset dataset = datasetCodec.deserialize(bytes, null);
        Quad found = dataset.stream().findFirst().orElseThrow(AssertionError::new);
        assertEquals("Found wrong quad!", quad, found.toString());
    }
}
