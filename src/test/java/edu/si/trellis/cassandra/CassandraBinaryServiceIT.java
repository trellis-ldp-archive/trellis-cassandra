package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.CountingInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraBinaryServiceIT extends Assert {

    private static final Logger log = LoggerFactory.getLogger(CassandraBinaryServiceIT.class);

    protected static int port = Integer.getInteger("cassandra.nativeTransportPort");
    
    protected static boolean cleanBefore = Boolean.getBoolean("cleanBeforeTests");
    protected static boolean cleanAfter = Boolean.getBoolean("cleanAfterTests");
    
    protected RDF rdfFactory = new SimpleRDF();
    
    @ClassRule
    public static final CassandraConnection connection = new CassandraConnection("127.0.0.1", port, "Trellis",
            cleanBefore, cleanAfter);
    
    @Test
    public void testSetAndGetSmallContent() throws IOException, InterruptedException {
    	IRI id = createIRI("http://example.com/id");      
    	CharSequence content = "This is only a short test, but it has meaning";
    	InputStream testInput = IOUtils.toInputStream(content, "utf-8");
        connection.binaryService.setContent(id, testInput);
        
        assertTrue("Binary must exist in storage.", connection.binaryService.exists(id));
        
        Optional<InputStream> got = connection.binaryService.getContent(id);
        assertTrue(got.isPresent());
        String reply = IOUtils.toString(got.get(), "utf-8");
        assertEquals(content, reply);

        Optional<InputStream> got2 = connection.binaryService.getContent(id, 5, 11);
        assertTrue(got2.isPresent());
        InputStream is = got2.get();
        String reply2 = IOUtils.toString(is, UTF_8);
        assertEquals(content.subSequence(5, 12), reply2);
    }
    
    @Test
    public void testSetAndGetMultiChunkContent() throws IOException, InterruptedException {
    	IRI id = createIRI("http://example.com/id2");
    	final String md5sum = "89c4b71c69f59cde963ce8aa9dbe1617";
    	FileInputStream fis = new FileInputStream("src/test/resources/test.jpg");
    	CountingInputStream cis = new CountingInputStream(fis);
        connection.binaryService.setContent(id, cis);
        long bytesWritten = cis.getCount();
        
        Optional<InputStream> got = connection.binaryService.getContent(id);
        assertTrue(got.isPresent());
        InputStream is = got.get();
        CountingInputStream counting = new CountingInputStream(is);
        String digest = DigestUtils.md5Hex(counting);
        assertEquals(bytesWritten, counting.getCount());
        assertEquals(md5sum, digest);
    }
    
    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }
}
