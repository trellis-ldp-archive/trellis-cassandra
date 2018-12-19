package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.CassandraBinaryService.CASSANDRA_CHUNK_HEADER_NAME;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;
import static org.apache.commons.io.IOUtils.contentEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;
import org.trellisldp.api.RuntimeTrellisException;

public class CassandraBinaryServiceIT extends CassandraServiceIT {

    private static final Logger log = getLogger(CassandraBinaryServiceIT.class);

    @Test
    public void setAndGetSmallContent() throws Exception {
        IRI id = createIRI();
        log.debug("Using identifier: {} for testSetAndGetSmallContent", id);
        String content = "This is only a short test, but it has meaning";
        try (InputStream testInput = IOUtils.toInputStream(content, UTF_8)) {
            connection.binaryService.setContent(builder(id).build(), testInput, emptyMap()).get();
        }

        try (InputStream got = connection.binaryService.get(id).get().getContent()) {
            String reply = IOUtils.toString(got, UTF_8);
            assertEquals(content, reply);
        }

        try (InputStream got = connection.binaryService.get(id).get().getContent(5, 11)) {
            String reply = IOUtils.toString(got, UTF_8);
            assertEquals(content.subSequence(5, 12), reply);
        }

        connection.binaryService.purgeContent(id).get();

        try {
            connection.binaryService.get(id).get();
            fail();
        } catch (Exception e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof NullPointerException);
            assertTrue(cause.getMessage().contains(id.getIRIString()));
        }
    }

    @Test
    public void setAndGetMultiChunkContent() throws Exception {
        IRI id = createIRI();
        final String md5sum = "89c4b71c69f59cde963ce8aa9dbe1617";
        try (FileInputStream testData = new FileInputStream("src/test/resources/test.jpg")) {
            connection.binaryService.setContent(builder(id).build(), testData, null).get();
        }

        CompletableFuture<Binary> got = connection.binaryService.get(id);
        Binary binary = got.get();
        assertTrue(got.isDone());

        try (InputStream testData = new FileInputStream("src/test/resources/test.jpg");
             InputStream content = binary.getContent()) {
            assertTrue(contentEquals(testData, content), "Didn't retrieve correct content!");
        }

        try (InputStream content = binary.getContent()) {
            String digest = DigestUtils.md5Hex(content);
            assertEquals(md5sum, digest);
        }

        String md5 = "d41d8cd98f00b204e9800998ecf8427e";
        MessageDigest d = DigestUtils.getMd5Digest();
        connection.binaryService.calculateDigest(id, d);
        assertEquals(md5, Hex.encodeHexString(d.digest()));
    }

    @Test
    public void varyChunkSizeFromDefault() throws IOException, InterruptedException, ExecutionException {
        IRI id = createIRI();
        final String chunkSize = "10000000";
        final String md5sum = "89c4b71c69f59cde963ce8aa9dbe1617";
        try (FileInputStream testData = new FileInputStream("src/test/resources/test.jpg")) {
            Map<String, List<String>> hints = ImmutableMap.of(CASSANDRA_CHUNK_HEADER_NAME, ImmutableList.of(chunkSize));
            connection.binaryService.setContent(builder(id).build(), testData, hints).get();
        }

        CompletableFuture<Binary> got = connection.binaryService.get(id);
        Binary binary = got.get();
        assertTrue(got.isDone());

        try (InputStream testData = new FileInputStream("src/test/resources/test.jpg");
             InputStream content = binary.getContent()) {
            assertTrue(contentEquals(testData, content), "Didn't retrieve correct content!");
        }

        try (InputStream content = binary.getContent()) {
            String digest = DigestUtils.md5Hex(content);
            assertEquals(md5sum, digest);
        }

        assertEquals(parseInt(chunkSize), ((CassandraBinary) binary).chunkLength());

        try (FileInputStream testData = new FileInputStream("src/test/resources/test.jpg")) {
            Map<String, List<String>> hints = ImmutableMap.of(CASSANDRA_CHUNK_HEADER_NAME,
                            ImmutableList.of(chunkSize, chunkSize + 1000));
            try {
                connection.binaryService.setContent(builder(id).build(), testData, hints).get();
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeTrellisException);
                assertTrue(e.getMessage().contains(CASSANDRA_CHUNK_HEADER_NAME));
            }
        }
    }

    private IRI createIRI() {
        return rdfFactory.createIRI("http://example.com/" + randomUUID());
    }
}
