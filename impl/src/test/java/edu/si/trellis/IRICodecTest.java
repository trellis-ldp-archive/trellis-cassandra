package edu.si.trellis;

import static edu.si.trellis.IRICodec.IRI_CODEC;
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
        assertThrows(IllegalArgumentException.class, () -> IRI_CODEC.parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh"));
    }

    @Test
    void testParse() {
        IRI iri = rdf.createIRI("http://example.com");
        String fieldForm = iri.getIRIString();
        assertEquals(iri, IRI_CODEC.parse(fieldForm));
    }

    @Test
    void testFormat() {
        IRI iri = rdf.createIRI("http://example.com");
        String fieldForm = iri.getIRIString();
        assertEquals(fieldForm, IRI_CODEC.format(iri));
    }

    @Test
    void testDeserialize() {
        IRI iri = rdf.createIRI("http://example.com");
        ByteBuffer fieldForm = ByteBuffer.wrap(iri.getIRIString().getBytes(UTF_8));
        assertEquals(iri, IRI_CODEC.decode(fieldForm, null));
    }

    @Test
    void nullForNull() {
        assertEquals(null, IRI_CODEC.parse(null));
        assertEquals(null, IRI_CODEC.format(null));
        assertEquals(null, IRI_CODEC.encode(null, null));
        assertEquals(null, IRI_CODEC.decode(null, null));
    }
}
