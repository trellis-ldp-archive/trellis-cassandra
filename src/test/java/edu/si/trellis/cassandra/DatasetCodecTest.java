package edu.si.trellis.cassandra;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidTypeException;

public class DatasetCodecTest extends Assert {

    private final DatasetCodec codec = new DatasetCodec();
    
    @Test(expected = InvalidTypeException.class)
    public void badParse() {
        String badRDF = "dfshgou;sdfhgoudfhogh";
        codec.parse(badRDF);
    }

    @Test
    public void testParse() {
        String quad = "<s> <p> <o> <g>.";
        Dataset dataset = codec.parse(quad);
        Quad found = dataset.stream().findFirst().orElseThrow(AssertionError::new);
        assertEquals("Found wrong quad!", quad, found.toString());
    }

    @Test
    public void testDeserialize() {
        String quad = "<s> <p> <o> <g>.";
        ByteBuffer bytes = ByteBuffer.wrap(quad.getBytes());
        Dataset dataset = codec.deserialize(bytes, null);
        Quad found = dataset.stream().findFirst().orElseThrow(AssertionError::new);
        assertEquals("Found wrong quad!", quad, found.toString());
    }
    
    @Test
    public void nullOrEmptyForNull() {
        assertEquals(0, codec.parse(null).size());
        assertEquals(null, codec.format(null));
        assertEquals(null, codec.serialize(null, null));
        assertEquals(0, codec.deserialize(null, null).size());
    }
}
