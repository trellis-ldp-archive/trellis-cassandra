package edu.si.trellis;

import static edu.si.trellis.DatasetCodec.DATASET_CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.jena.riot.RiotException;
import org.junit.jupiter.api.Test;

class DatasetCodecTest {

    private static final RDF rdf = new SimpleRDF();

    @Test
    void badParse() {
        assertThrows(RiotException.class, () -> DATASET_CODEC.parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh"));
    }

    @Test
    void testParse() throws Exception {
        String nQuad1 = "<s> <p> <o> <g> .";
        Quad q1 = quad(iri("g"), iri("s"), iri("p"), iri("o"));
        String nQuad2 = "<s1> <p1> \"foo\" .";
        Quad q2 = quad(null, iri("s1"), iri("p1"), rdf.createLiteral("foo"));
        String nQuad3 = "<s> <p> <o> <g2> .";
        Quad q3 = quad(iri("g2"), iri("s"), iri("p"), iri("o"));
        String nQuads = String.join("\n", nQuad1, nQuad2, nQuad3);
        try (Dataset dataset = DATASET_CODEC.parse(nQuads)) {
            assertEquals(3, dataset.size());
            for (Quad q : new Quad[] { q1, q2, q3 })
                assertTrue(dataset.contains(q));
        }
    }

    @Test
    void testDeserialize() throws Exception {
        String nQuad1 = "<s> <p> <o> <g> .";
        Quad q1 = quad(iri("g"), iri("s"), iri("p"), iri("o"));
        String nQuad2 = "<s1> <p1> \"foo\" .";
        Quad q2 = quad(null, iri("s1"), iri("p1"), rdf.createLiteral("foo"));
        String nQuad3 = "<s> <p> <o> <g2> .";
        Quad q3 = quad(iri("g2"), iri("s"), iri("p"), iri("o"));
        ByteBuffer nQuads = ByteBuffer.wrap(String.join("\n", nQuad1, nQuad2, nQuad3).getBytes(UTF_8));
        try (Dataset dataset = DATASET_CODEC.decode(nQuads, null)) {
            assertEquals(3, dataset.size());
            for (Quad q : new Quad[] { q1, q2, q3 })
                assertTrue(dataset.contains(q));
        }
    }

    @Test
    void testFormat() throws Exception {
        String nQuad = "<s> <p> <o> <g> .";
        Quad q = quad(iri("g"), iri("s"), iri("p"), iri("o"));
        try (Dataset dataset = rdf.createDataset()) {
            dataset.add(q);
            String nQuads = DATASET_CODEC.format(dataset);
            assertEquals(1, dataset.size());
            assertEquals(nQuad, nQuads.trim());
        }
    }

    @Test
    void edgeCase1() {
        assertEquals(0, DATASET_CODEC.parse(null).size());
    }

    @Test
    void edgeCase2() {
        assertEquals(null, DATASET_CODEC.format(null));
    }

    @Test
    void edgeCase3() throws Exception {
        try (Dataset empty = rdf.createDataset()) {
            assertEquals(null, DATASET_CODEC.format(empty));
        }
    }

    @Test
    void edgeCase4() throws Exception {
        try (Dataset empty = rdf.createDataset()) {
            assertEquals(null, DATASET_CODEC.encode(empty, null));
        }
    }

    @Test
    void edgeCase5() {
        assertEquals(null, DATASET_CODEC.encode(null, null));
    }

    @Test
    void edgeCase6() {
        assertEquals(0, DATASET_CODEC.decode(null, null).size());
    }

    @Test
    void badData() {
        assertThrows(RiotException.class, () -> DATASET_CODEC.encode(new BadDataset(), null));
    }

    private Quad quad(BlankNodeOrIRI g, BlankNodeOrIRI s, IRI p, RDFTerm o) {
        return rdf.createQuad(g, s, p, o);
    }

    private IRI iri(String v) {
        return rdf.createIRI(v);
    }

    /**
     * {@link #stream()} throws a {@code RiotException} for testing.
     *
     */
    private static final class BadDataset implements Dataset {
        @Override
        public void add(Quad quad) {}

        @Override
        public void add(BlankNodeOrIRI graphName, BlankNodeOrIRI subject, IRI predicate, RDFTerm object) {}

        @Override
        public boolean contains(Quad quad) {
            return false;
        }

        @Override
        public boolean contains(Optional<BlankNodeOrIRI> graphName, BlankNodeOrIRI subject, IRI predicate,
                        RDFTerm object) {
            return false;
        }

        @Override
        public Graph getGraph() {
            return null;
        }

        @Override
        public Optional<Graph> getGraph(BlankNodeOrIRI graphName) {
            return null;
        }

        @Override
        public Stream<BlankNodeOrIRI> getGraphNames() {
            return null;
        }

        @Override
        public void remove(Quad quad) {}

        @Override
        public void remove(Optional<BlankNodeOrIRI> graphName, BlankNodeOrIRI subject, IRI predicate, RDFTerm object) {}

        @Override
        public void clear() {}

        @Override
        public long size() {
            return 1;
        }

        @Override
        public Stream<? extends Quad> stream() {
            throw new RiotException();
        }

        @Override
        public Stream<? extends Quad> stream(Optional<BlankNodeOrIRI> graphName, BlankNodeOrIRI subject, IRI predicate,
                        RDFTerm object) {
            return null;
        }
    }
}
