package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class DatasetCodecTest extends Assert {

    Logger log = LoggerFactory.getLogger(DatasetCodecTest.class);

    @Test(expected = InvalidTypeException.class)
    public void badQuads() {
        DatasetCodec datasetCodec = new DatasetCodec(DataType.text());
        String badQuads = "dfshgou;sdfhgoudfhogh";
        datasetCodec.parse(badQuads);
    }

    @Test
    public void testParse() {
        DatasetCodec datasetCodec = new DatasetCodec(DataType.text());
        String quad = "<s> <p> <o> <g>.";
        Dataset dataset = datasetCodec.parse(quad);
        Quad found = dataset.stream().findFirst().orElseThrow(() -> new AssertionError("Didn't find the quad!"));
        assertEquals("Found wrong quad!", quad, found.toString());
    }
}
