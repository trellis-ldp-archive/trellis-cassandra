package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.CountingInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.Range;
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
        
        Range<Integer> range = Range.between(5, 11); // inclusive
        List<Range<Integer>> ranges = Collections.singletonList(range);
        Optional<InputStream> got2 = connection.binaryService.getContent(id, ranges);
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
    
    @Test
    public void testSplitByRangeGet() throws IOException, InterruptedException {
    	IRI id = createIRI("http://example.com/id3");
    	FileInputStream fis = new FileInputStream("src/test/resources/test.jpg");
    	BoundedInputStream bounded = new BoundedInputStream(fis, 1048576+1);
    	for(int i = 0; i < 500; i++) bounded.read();
    	CountingInputStream cis = new CountingInputStream(bounded);
    	String rangedigest = DigestUtils.md5Hex(cis);
    	long byteCount = cis.getCount();
    	FileInputStream input = new FileInputStream("src/test/resources/test.jpg");
        connection.binaryService.setContent(id, input);
        
        List<Range<Integer>> ranges = new ArrayList<>();
        // bytes=500-835583
        // bytes=835584-1048576  (1 * 1024 * 1024)
        // FIXME: Range requests can be one sided, i.e. bytes=-900 for last 900 bytes, or 900- for bytes 900 on.
        ranges.add(Range.between(500, 835583));
        ranges.add(Range.between(835584, 1048576));
        
        Optional<InputStream> got = connection.binaryService.getContent(id, ranges);
        assertTrue(got.isPresent());
        
        CountingInputStream counting = new CountingInputStream(got.get());
        String digest = DigestUtils.md5Hex(counting);
        assertEquals(byteCount, counting.getCount());
        assertEquals(rangedigest, digest);
    }
    
    private IRI createIRI(String iri) {
        return rdfFactory.createIRI(iri);
    }
}
