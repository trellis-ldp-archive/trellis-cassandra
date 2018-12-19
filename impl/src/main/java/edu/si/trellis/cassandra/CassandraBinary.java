package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;

/**
 * Simple implementation of {@link Binary} that pulls content from Cassandra on demand.
 *
 */
public class CassandraBinary implements Binary {

    private static final Logger log = getLogger(CassandraBinary.class);

    private final IRI id;

    private final BinaryQueryContext context;

    private final int chunkLength;

    /**
     * @param id identifier for this {@link Binary}
     * @param c context for queries
     * @param chunkLength the length of chunk to use reading bits from Cassandra
     */
    public CassandraBinary(IRI id, BinaryQueryContext c, int chunkLength) {
        this.id = id;
        this.context = c;
        this.chunkLength = chunkLength;
    }

    @Override
    public InputStream getContent() {
        return context.read(id);
    }

    @Override
    public BoundedInputStream getContent(int from, int to) {
        int firstChunk = from / chunkLength;
        int lastChunk = to / chunkLength;
        int chunkStreamStart = from % chunkLength;
        int rangeSize = to - from + 1; // +1 because range is inclusive
        try (InputStream retrieve = context.readRange(id, firstChunk, lastChunk)) {
            retrieve.skip(chunkStreamStart); // skip to fulfill lower end of range
            return new BoundedInputStream(retrieve, rangeSize); // apply limit for upper end of range
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * @return the length of chunk used by {@code this}
     */
    public int chunkLength() {
        return chunkLength;
    }
}
