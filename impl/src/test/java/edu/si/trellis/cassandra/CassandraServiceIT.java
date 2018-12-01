package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.jupiter.api.extension.RegisterExtension;

public class CassandraServiceIT {

    protected RDF rdfFactory = new SimpleRDF();
    
    @RegisterExtension
    protected static CassandraConnection connection = new CassandraConnection();

}
