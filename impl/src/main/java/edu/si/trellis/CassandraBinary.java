package edu.si.trellis;

import static org.slf4j.LoggerFactory.getLogger;

import edu.si.trellis.query.binary.Read;
import edu.si.trellis.query.binary.ReadRange;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionStage;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;

/**
 * Simple implementation of {@link Binary} that pulls content from Cassandra on demand.
 *
 */
public class CassandraBinary implements Binary {

    private final IRI id;

    private final int chunkLength;

    private final Read read;

    private final ReadRange readRange;
    
    private static final Logger log = getLogger(CassandraBinary.class);

    /**
     * @param id identifier for this {@link Binary}
     * @param read a {@link Read} query to use
     * @param readRange a {@link ReadRange} query to use
     * @param chunkLength the length of chunk to use reading bits from Cassandra
     */
    public CassandraBinary(IRI id, Read read, ReadRange readRange, int chunkLength) {
        this.id = id;
        this.read = read;
        this.readRange = readRange;
        if (chunkLength < 1) throw new IllegalArgumentException("Chunk length < 1!");
        this.chunkLength = chunkLength;
    }

    @Override
    public CompletionStage<InputStream> getContent() {
        return read.execute(id);
    }

    @Override
    public CompletionStage<InputStream> getContent(int from, int to) {
        int firstChunk = from / chunkLength;
        int lastChunk = to / chunkLength;
        int chunkStreamStart = from % chunkLength;
        int rangeSize = to - from + 1; // +1 because range is inclusive
        return readRange.execute(id, firstChunk, lastChunk)
                        // skip to fulfill lower end of range
                        .thenApply(retrieve -> {
                            try {
                                log.trace("Skipping {} bytes…", chunkStreamStart);
                                // we needn't check the result; see BinaryReadQuery#retrieve
                                retrieve.skip(chunkStreamStart);
                                log.trace("Done skipping {} bytes.", chunkStreamStart);                               
                                return retrieve;
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            // apply limit for upper end of range
                        }).thenApply(retrieve -> new BoundedInputStream(retrieve, rangeSize));
    }
}
