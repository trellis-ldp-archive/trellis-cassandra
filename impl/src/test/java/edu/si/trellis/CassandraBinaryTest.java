package edu.si.trellis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.si.trellis.query.binary.Read;
import edu.si.trellis.query.binary.ReadRange;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.trellisldp.api.RuntimeTrellisException;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("resource")
class CassandraBinaryTest {

    private final RDF factory = new SimpleRDF();

    private int testChunkSize;

    private final IRI testId = factory.createIRI("urn:test");

    @Mock
    private Read mockRead;

    @Mock
    private ReadRange mockReadRange;

    @Test
    void noContent() {
        CassandraBinary testCassandraBinary = new CassandraBinary(testId, mockRead, mockReadRange, testChunkSize);

        try {
            testCassandraBinary.getContent();
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeTrellisException, "Wrong exception type!");
            assertEquals("Binary not found under IRI: urn:test", e.getMessage(), "Wrong exception message!");
        }
    }
}
