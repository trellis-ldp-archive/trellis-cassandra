package edu.si.trellis.cassandra;

import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Statement;

import edu.si.trellis.cassandra.CassandraBinaryService.BinaryQueryContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;
import org.trellisldp.api.RuntimeTrellisException;

/**
 * Simple implementation of {@link Binary} that pulls content from Cassandra on demand.
 *
 */
public class CassandraBinary implements Binary {

    private static final Logger log = getLogger(CassandraBinary.class);

    private final Long size;

    private final IRI id;

    private final BinaryQueryContext context;

    private final int chunkLength;

    /**
     * @param id identifier for this {@link Binary}
     * @param size size in bytes of this {@code Binary}
     * @param c context for queries
     * @param length the length of chunk to use in Cassandra
     */
    public CassandraBinary(IRI id, Long size, BinaryQueryContext c, int length) {
        this.id = id;
        this.size = size;
        this.context = c;
        this.chunkLength = length;
    }

    @Override
    public Long getSize() {
        return size;
    }

    @Override
    public InputStream getContent() {
        return retrieve(context.readStatement().bind(id));
    }

    @Override
    public BoundedInputStream getContent(int from, int to) {
        int firstChunk = from / chunkLength;
        int lastChunk = to / chunkLength;
        int chunkStreamStart = from % chunkLength;
        int rangeSize = to - from + 1; // +1 because range is inclusive
        Statement boundStatement = context.readRangeStatement().bind(id.getIRIString(), firstChunk, lastChunk);

        try (InputStream retrieve = retrieve(boundStatement)) {
            retrieve.skip(chunkStreamStart); // skip to fulfill lower end of range
            return new BoundedInputStream(retrieve, rangeSize); // apply limit for upper end of range
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //@formatter:off
    private InputStream retrieve(Statement statement) {
        return stream(context.session().execute(statement).spliterator(), false)
                        .map(r -> r.getInt("chunk_index"))
                        .peek(chunkNumber -> log.debug("Found pointer to chunk: {}", chunkNumber))
                        .map(chunkNumber -> context.readSingleChunk().bind(id, chunkNumber))
                        .<InputStream> map(s -> new LazyChunkInputStream(context.session(), s))
                        .reduce(SequenceInputStream::new) // chunks now in one large stream
                        .orElseThrow(() -> new RuntimeTrellisException("Binary not found under IRI: " + id.getIRIString()));
    }
    //@formatter:on
}
