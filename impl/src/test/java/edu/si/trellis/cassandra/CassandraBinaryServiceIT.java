package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;

import com.google.common.io.CountingInputStream;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class CassandraBinaryServiceIT extends CassandraServiceIT {

    private static final Logger log = getLogger(CassandraBinaryServiceIT.class);

    @Test
    public void testSetAndGetSmallContent() throws Exception {
        IRI id = createIRI();
        String content = "This is only a short test, but it has meaning";
        InputStream testInput = IOUtils.toInputStream(content, UTF_8);
        connection.binaryService.setContent(builder(id).build(), testInput);

        InputStream got = connection.binaryService.getContent(id).get();
        String reply = IOUtils.toString(got, UTF_8);
        assertEquals(content, reply);

        InputStream got2 = connection.binaryService.getContent(id, 5, 11).get();
        String reply2 = IOUtils.toString(got2, UTF_8);
        assertEquals(content.subSequence(5, 12), reply2);
    }

    @Test
    public void testSetAndGetMultiChunkContent() throws Exception {
        IRI id = createIRI();
        final String md5sum = "89c4b71c69f59cde963ce8aa9dbe1617";
        FileInputStream fis = new FileInputStream("src/test/resources/test.jpg");
        CountingInputStream cis = new CountingInputStream(fis);
        connection.binaryService.setContent(builder(id).build(), cis);
        long bytesWritten = cis.getCount();

        CompletableFuture<InputStream> got = connection.binaryService.getContent(id);
        InputStream is = got.get();
        assertTrue(got.isDone());
        CountingInputStream counting = new CountingInputStream(is);
        String digest = DigestUtils.md5Hex(counting);
        assertEquals(bytesWritten, counting.getCount());
        assertEquals(md5sum, digest);
    }

    @Test
    public void badDigestAlgorithmChoice() {
        assertThrows(IllegalArgumentException.class,
                        () -> connection.binaryService.calculateDigest(createIRI(), "BAD DIGEST ALGORITHM!!!"));
    }

    private IRI createIRI() {
        return rdfFactory.createIRI("http://example.com/" + randomUUID());
    }
}
