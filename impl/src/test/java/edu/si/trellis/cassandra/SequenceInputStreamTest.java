package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class SequenceInputStreamTest extends Assert {

    @Test
    public void shouldConcatStreams() throws IOException {
        try (InputStream one = new ByteArrayInputStream("one".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("two".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            String answer = IOUtils.toString(stream, UTF_8);
            assertEquals("Did not correctly concat streams!", "onetwo", answer);
        }
    }

    @Test
    public void shouldSkipAcrossStreams() throws IOException {
        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            assertEquals("Could not skip through first stream!", 12, stream.skip(12));
            String answer = IOUtils.toString(stream, UTF_8);
            assertEquals("Did not correctly stream rest of streams!", "ourfivesix", answer);
        }
    }

    @Test
    public void shouldReadAcrossStreams() throws IOException {
        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two);
                        InputStream answer = new ByteArrayInputStream(
                                        ("onetwothree" + "fourfivesix").getBytes(UTF_8))) {
            int read, count = 0;
            while ((read = stream.read()) != -1) {
                count++;
                assertEquals("Got wrong byte from read()!", read, answer.read());
            }
            assertEquals("Answer stream was not exhausted after comparison!", answer.read(), -1);
            assertEquals("Not enough bytes were read!", count, 22);
        }
    }

    @Test
    public void shouldFulfillSkipContractEdges() throws IOException {
        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            assertEquals(0, stream.skip(0));
            assertEquals(0, stream.skip(-1));
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldFulfillReadContractEdges1() throws IOException {
        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            stream.read(new byte[0], 0, -1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldFulfillReadContractEdges2() throws IOException {
        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            stream.read(new byte[0], -1, 10);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldFulfillReadContractEdges3() throws IOException {

        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            stream.read(new byte[5], 2, 10);
        }
    }

    @Test
    public void shouldFulfillReadContractEdges4() throws IOException {

        try (InputStream one = new ByteArrayInputStream("onetwothree".getBytes(UTF_8));
                        InputStream two = new ByteArrayInputStream("fourfivesix".getBytes(UTF_8));
                        SequenceInputStream stream = new SequenceInputStream(one, two)) {
            assertEquals(0, stream.read(new byte[5], 2, 0));
        }
    }
}
