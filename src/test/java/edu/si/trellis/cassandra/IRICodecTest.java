package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidTypeException;

public class IRICodecTest extends Assert {

    RDF rdf = new SimpleRDF();

    @Test(expected = InvalidTypeException.class)
    public void badParse() {
        new IRICodec().parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh");
    }

    @Test
    public void testParse() {
        IRI iri = rdf.createIRI("http://example.com");
        String fieldForm = "<" + iri.getIRIString() + ">";
        assertEquals(iri, new IRICodec().parse(fieldForm));
    }
    
    @Test
    public void nullForNull() {
        assertEquals(null, new IRICodec().parse(null));
        assertEquals(null, new IRICodec().format(null));
        assertEquals(null, new IRICodec().serialize(null, null));
        assertEquals(null, new IRICodec().deserialize(null, null));
    }
}
