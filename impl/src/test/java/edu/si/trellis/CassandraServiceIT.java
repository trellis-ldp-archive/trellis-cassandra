package edu.si.trellis;

import static java.lang.Boolean.TRUE;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.TWO_SECONDS;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.jupiter.api.extension.RegisterExtension;

class CassandraServiceIT {

    protected RDF rdfFactory = new SimpleRDF();

    @RegisterExtension
    protected static CassandraConnection connection = new CassandraConnection();

    protected IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }

    protected void waitTwoSeconds() {
        await().pollDelay(TWO_SECONDS).until(() -> TRUE);
    }
}
