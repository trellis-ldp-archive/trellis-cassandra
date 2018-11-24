package edu.si.trellis.cassandra;

import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static org.apache.commons.rdf.api.RDFSyntax.TURTLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.trellisldp.api.TrellisUtils.getInstance;
import static org.trellisldp.test.TestUtils.readEntityAsGraph;
import static org.trellisldp.vocabulary.RDF.type;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.test.MementoResourceTests;
import org.trellisldp.vocabulary.DC;
import org.trellisldp.vocabulary.SKOS;

public class MementoResourceIT extends IT implements MementoResourceTests {

    @Override
    
    public void testMementoContent() {
        final RDF rdf = getInstance();
        final Dataset dataset = rdf.createDataset();
        final Map<String, String> mementos = getMementos();
        mementos.forEach((memento, date) -> {
            try (final Response res = target(memento).request().get()) {
                assertEquals(SUCCESSFUL, res.getStatusInfo().getFamily(), "Check for a successful request");
                readEntityAsGraph(res.getEntity(), getBaseURL(), TURTLE).stream().forEach(triple ->
                        dataset.add(rdf.createIRI(memento), triple.getSubject(), triple.getPredicate(),
                            triple.getObject()));
            }
        });

        final IRI subject = rdf.createIRI(getResourceLocation());
        final List<IRI> urls = mementos.keySet().stream().sorted().map(rdf::createIRI).collect(toList());
        assertEquals(3L, urls.size(), "Check that three mementos were found");
        assertTrue(dataset.getGraph(urls.get(0)).isPresent(), "Check that the first graph is present");
        dataset.getGraph(urls.get(0)).ifPresent(g -> {
            assertTrue(g.contains(subject, type, SKOS.Concept), "Check for a skos:Concept type");
            assertTrue(g.contains(subject, SKOS.prefLabel, rdf.createLiteral("Resource Name", "eng")),
                    "Check for a skos:prefLabel property");
            assertTrue(g.contains(subject, DC.subject, rdf.createIRI("http://example.org/subject/1")),
                    "Check for a dc:subject property");
            assertEquals(3L, g.size(), "Check for three triples");
        });

        assertTrue(dataset.getGraph(urls.get(1)).isPresent(), "Check that the second graph is present");
        dataset.getGraph(urls.get(1)).ifPresent(g -> {
            assertTrue(g.contains(subject, type, SKOS.Concept), "Check for a skos:Concept type");
            assertTrue(g.contains(subject, SKOS.prefLabel, rdf.createLiteral("Resource Name", "eng")),
                    "Check for a skos:prefLabel property");
            assertTrue(g.contains(subject, DC.subject, rdf.createIRI("http://example.org/subject/1")),
                    "Check for a dc:subject property");
            assertTrue(g.contains(subject, DC.title, rdf.createLiteral("Title")),
                    "Check for a dc:title property");
            assertEquals(4L, g.size(), "Check for four triples");
        });

        assertTrue(dataset.getGraph(urls.get(2)).isPresent(), "Check that the third graph is present");
        dataset.getGraph(urls.get(2)).ifPresent(g -> {
            assertTrue(g.contains(subject, type, SKOS.Concept), "Check for a skos:Concept type");
            assertTrue(g.contains(subject, SKOS.prefLabel, rdf.createLiteral("Resource Name", "eng")),
                    "Check for a skos:prefLabel property");
            assertTrue(g.contains(subject, DC.subject, rdf.createIRI("http://example.org/subject/1")),
                    "Check for a dc:subject property");
            assertTrue(g.contains(subject, DC.title, rdf.createLiteral("Title")),
                    "Check for a dc:title property");
            assertTrue(g.contains(subject, DC.alternative, rdf.createLiteral("Alternative Title")),
                    "Check for a dc:alternative property");
            assertEquals(5L, g.size(), "Check for five triples");
        });
    }
}
