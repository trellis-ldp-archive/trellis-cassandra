package edu.si.trellis;

import static edu.si.trellis.IRICodec.iriCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.jupiter.api.Test;

class IRICodecTest {

    RDF rdf = new SimpleRDF();

    @Test
    void badParse() {
        assertThrows(IllegalArgumentException.class, () -> iriCodec.parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh"));
    }

    @Test
    void testParse() {
        IRI iri = rdf.createIRI("http://example.com");
        String fieldForm = iri.getIRIString();
        assertEquals(iri, iriCodec.parse(fieldForm));
    }

    @Test
    void testFormat() {
        IRI iri = rdf.createIRI("http://example.com");
        String fieldForm = iri.getIRIString();
        assertEquals(fieldForm, iriCodec.format(iri));
    }

    @Test
    void testDeserialize() {
        IRI iri = rdf.createIRI("http://example.com");
        ByteBuffer fieldForm = ByteBuffer.wrap(iri.getIRIString().getBytes(UTF_8));
        assertEquals(iri, iriCodec.decode(fieldForm, null));
    }

    @Test
    void nullForNull() {
        assertEquals(null, iriCodec.parse(null));
        assertEquals(null, iriCodec.format(null));
        assertEquals(null, iriCodec.encode(null, null));
        assertEquals(null, iriCodec.decode(null, null));
    }
}
