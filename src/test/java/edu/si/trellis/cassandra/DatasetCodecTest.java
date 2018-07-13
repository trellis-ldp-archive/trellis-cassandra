package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;

import com.datastax.driver.core.exceptions.InvalidTypeException;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.Test;

public class DatasetCodecTest extends Assert {

    private static final RDF rdf = new SimpleRDF();

    @Test(expected = InvalidTypeException.class)
    public void badParse() {
        datasetCodec.parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh");
    }

    @Test
    public void testParse() throws Exception {
        String nQuad1 = "<s> <p> <o> <g> .";
        Quad q1 = quad(iri("g"), iri("s"), iri("p"), iri("o"));
        String nQuad2 = "<s1> <p1> \"foo\" .";
        Quad q2 = quad(null, iri("s1"), iri("p1"), rdf.createLiteral("foo"));
        String nQuad3 = "<s> <p> <o> <g2> .";
        Quad q3 = quad(iri("g2"), iri("s"), iri("p"), iri("o"));
        String nQuads = String.join("\n", nQuad1, nQuad2, nQuad3);
        try (Dataset dataset = datasetCodec.parse(nQuads)) {
            assertEquals(3, dataset.size());
            for (Quad q : new Quad[] { q1, q2, q3 })
                assertTrue(dataset.contains(q));
        }
    }

    @Test
    public void nullForNull() {
        assertEquals(0, datasetCodec.parse(null).size());
        assertEquals(null, datasetCodec.format(null));
        assertEquals(null, datasetCodec.serialize(null, null));
        assertEquals(0, datasetCodec.deserialize(null, null).size());
    }

    private Quad quad(BlankNodeOrIRI g, BlankNodeOrIRI s, IRI p, RDFTerm o) {
        return rdf.createQuad(g, s, p, o);
    }

    private IRI iri(String v) {
        return rdf.createIRI(v);
    }
}
