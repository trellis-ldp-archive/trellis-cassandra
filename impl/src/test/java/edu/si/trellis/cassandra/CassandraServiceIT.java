package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.ClassRule;

public class CassandraServiceIT extends Assert {

    private static String contactHost = System.getProperty("cassandra.contactHost","127.0.0.1");

    protected static int port = Integer.getInteger("cassandra.nativeTransportPort");
    
    protected static boolean cleanBefore = Boolean.getBoolean("cleanBeforeTests");
    protected static boolean cleanAfter = Boolean.getBoolean("cleanAfterTests");
    
    protected RDF rdfFactory = new SimpleRDF();

    /**
     * Connects to test cluster.
     */
    @ClassRule
    public static final CassandraConnection connection = new CassandraConnection(contactHost, port, "Trellis",
            cleanBefore, cleanAfter);
}
