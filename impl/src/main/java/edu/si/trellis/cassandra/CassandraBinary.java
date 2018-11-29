package edu.si.trellis.cassandra;

import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Statement;

import edu.si.trellis.cassandra.CassandraBinaryService.BinaryContext;

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

    private final BinaryContext context;

    /**
     * @param id identifier for this {@link Binary}
     * @param size size in bytes of this  {@code Binary}
     * @param c context for queries 
     */
    public CassandraBinary(IRI id, Long size, BinaryContext c) {
        this.id = id;
        this.size = size;
        this.context = c;
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
        int firstChunk = from / context.maxChunkLength();
        int lastChunk = to / context.maxChunkLength();
        int chunkStreamStart = from % context.maxChunkLength();
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
    private InputStream retrieve(Statement boundStatement) {
        return stream(context.session().execute(boundStatement.setConsistencyLevel(context.readConsistency())).spliterator(), false)
                        .map(r -> r.getInt("chunk_index"))
                        .peek(chunkNumber -> log.debug("Found pointer to chunk: {}", chunkNumber))
                        .map(chunkNumber -> context.readSingleChunk().bind(id, chunkNumber))
                        .<InputStream> map(statement -> new LazyChunkInputStream(context.session(), statement))
                        .reduce(SequenceInputStream::new) // chunks now in one large stream
                        .orElseThrow(() -> new RuntimeTrellisException("Binary not found under IRI: " + id.getIRIString()));
    }
    //@formatter:on
}
