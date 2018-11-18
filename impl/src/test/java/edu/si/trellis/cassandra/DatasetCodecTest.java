package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.DatasetCodec.datasetCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.driver.core.exceptions.InvalidTypeException;

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

public class DatasetCodecTest {

    private static final RDF rdf = new SimpleRDF();

    @Test
    public void badParse() {
        assertThrows(InvalidTypeException.class, () -> datasetCodec.parse("SGDF   &&$$$dfshgou;sdfhgoudfhogh"));
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
    public void testDeserialize() throws Exception {
        String nQuad1 = "<s> <p> <o> <g> .";
        Quad q1 = quad(iri("g"), iri("s"), iri("p"), iri("o"));
        String nQuad2 = "<s1> <p1> \"foo\" .";
        Quad q2 = quad(null, iri("s1"), iri("p1"), rdf.createLiteral("foo"));
        String nQuad3 = "<s> <p> <o> <g2> .";
        Quad q3 = quad(iri("g2"), iri("s"), iri("p"), iri("o"));
        ByteBuffer nQuads = ByteBuffer.wrap(String.join("\n", nQuad1, nQuad2, nQuad3).getBytes(UTF_8));
        try (Dataset dataset = datasetCodec.deserialize(nQuads, null)) {
            assertEquals(3, dataset.size());
            for (Quad q : new Quad[] { q1, q2, q3 })
                assertTrue(dataset.contains(q));
        }
    }

    @Test
    public void testFormat() throws Exception {
        String nQuad = "<s> <p> <o> <g> .";
        Quad q = quad(iri("g"), iri("s"), iri("p"), iri("o"));
        try (Dataset dataset = rdf.createDataset()) {
            dataset.add(q);
            String nQuads = datasetCodec.format(dataset);
            assertEquals(1, dataset.size());
            assertEquals(nQuad, nQuads.trim());
        }
    }

    @Test
    public void edgeCase1() throws Exception {
        assertEquals(0, datasetCodec.parse(null).size());
    }

    @Test
    public void edgeCase2() {
        assertEquals(null, datasetCodec.format(null));
    }

    @Test
    public void edgeCase3() throws Exception {
        try (Dataset empty = rdf.createDataset()) {
            assertEquals(null, datasetCodec.format(empty));
        }
    }

    @Test
    public void edgeCase4() throws Exception {
        try (Dataset empty = rdf.createDataset()) {
            assertEquals(null, datasetCodec.serialize(empty, null));
        }
    }

    @Test
    public void edgeCase5() throws Exception {
        assertEquals(null, datasetCodec.serialize(null, null));
    }

    @Test
    public void edgeCase6() throws Exception {
        assertEquals(0, datasetCodec.deserialize(null, null).size());
    }

    @Test
    public void badData() {
        assertThrows(InvalidTypeException.class, () -> datasetCodec.serialize(new BadDataset(), null));
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
