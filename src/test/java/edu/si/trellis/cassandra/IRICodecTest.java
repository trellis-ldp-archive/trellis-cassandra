package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.IRICodec.iriCodec;

import com.datastax.driver.core.exceptions.InvalidTypeException;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.Test;

public class IRICodecTest extends Assert {

    RDF rdf = new SimpleRDF();

    @Test(expected = InvalidTypeException.class)
    public void badParse() {
        iriCodec.parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh");
    }

    @Test
    public void testParse() {
        IRI iri = rdf.createIRI("http://example.com");
        String fieldForm = iri.getIRIString();
        assertEquals(iri, iriCodec.parse(fieldForm));
    }

    @Test
    public void nullForNull() {
        assertEquals(null, iriCodec.parse(null));
        assertEquals(null, iriCodec.format(null));
        assertEquals(null, iriCodec.serialize(null, null));
        assertEquals(null, iriCodec.deserialize(null, null));
    }
}
