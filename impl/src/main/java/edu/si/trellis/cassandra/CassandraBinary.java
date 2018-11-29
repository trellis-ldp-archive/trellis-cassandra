package edu.si.trellis.cassandra;

import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

import edu.si.trellis.cassandra.CassandraBinaryService.BinaryQueries;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;

public class CassandraBinary implements Binary {

    private static final Logger log = getLogger(CassandraBinary.class);

    private final Long size;

    private final IRI id;

    private final BinaryQueries q;

    public CassandraBinary(IRI id, Long size, BinaryQueries queries) {
        this.id = id;
        this.size = size;
        this.q = queries;
    }

    @Override
    public Long getSize() {
        return size;
    }

    @Override
    public InputStream getContent() {
        return retrieve(q.readStatement().bind(id));
    }

    @Override
    public BoundedInputStream getContent(int from, int to) {
        requireNonNull(from, "Byte range component 'from' may not be null!");
        requireNonNull(to, "Byte range component 'to' may not be null!");
        int firstChunk = from / q.maxChunkLength();
        int lastChunk = to / q.maxChunkLength();
        int chunkStreamStart = from % q.maxChunkLength();
        int rangeSize = to - from + 1; // +1 because range is inclusive
        Statement boundStatement = q.readRangeStatement().bind(id.getIRIString(), firstChunk, lastChunk);

        // skip to fulfill lower end of range
        try (InputStream retrieve = retrieve(boundStatement)) {
            // we control the codec from
            retrieve.skip(chunkStreamStart);
            return new BoundedInputStream(retrieve, rangeSize); // apply limit for // range
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //@formatter:off
    private InputStream retrieve(Statement boundStatement) {
        return stream(q.session().execute(boundStatement.setConsistencyLevel(q.readConsistency())).spliterator(), false)
                        .map(r->r.getInt("chunk_index"))
                        .peek(chunkNumber -> log.debug("Found pointer to chunk: {}", chunkNumber))
                        .map(chunkNumber -> q.readSingleChunk().bind(id, chunkNumber))
                        .<InputStream> map(statement -> new LazyChunkInputStream(q.session(), statement))
                        .reduce(SequenceInputStream::new).get(); // chunks now in one large stream
    }
    //@formatter:on
}
