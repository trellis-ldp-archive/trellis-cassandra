package edu.si.trellis.cassandra;

import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.RuntimeTrellisException;

class BinaryQueryContext extends QueryContext {

    private static final Logger log = getLogger(BinaryQueryContext.class);

    private static final String INSERT_QUERY = "INSERT INTO " + BINARY_TABLENAME
                    + " (identifier, size, chunk_index, chunk) VALUES (:identifier, :size, :chunk_index, :chunk)";

    private static final String RETRIEVE_QUERY = "SELECT size FROM " + BINARY_TABLENAME
                    + " WHERE identifier = ? LIMIT 1;";

    private static final String DELETE_QUERY = "DELETE FROM " + BINARY_TABLENAME + " WHERE identifier = ?;";

    private static final String READ_ALL_QUERY = "SELECT chunk_index FROM " + BINARY_TABLENAME
                    + " WHERE identifier = ?;";

    private static final String READ_RANGE_QUERY = "SELECT chunk_index FROM " + BINARY_TABLENAME
                    + " WHERE identifier = ? and chunk_index >= :start and chunk_index <= :end;";

    private static final String READ_CHUNK_QUERY = "SELECT chunk FROM " + BINARY_TABLENAME
                    + " WHERE identifier = ? and chunk_index = :chunk_index;";

    private final PreparedStatement deleteStatement, insertStatement, retrieveStatement, readRangeStatement,
                    readStatement, readChunkStatement;

    @Inject
    public BinaryQueryContext(Session session, @BinaryReadConsistency ConsistencyLevel readConsistency,
                    @BinaryReadConsistency ConsistencyLevel writeConsistency) {
        super(session);
        this.insertStatement = session.prepare(INSERT_QUERY).setConsistencyLevel(writeConsistency);
        this.deleteStatement = session.prepare(DELETE_QUERY).setConsistencyLevel(writeConsistency);
        this.retrieveStatement = session.prepare(RETRIEVE_QUERY).setConsistencyLevel(readConsistency);
        this.readStatement = session.prepare(READ_ALL_QUERY).setConsistencyLevel(readConsistency);
        this.readRangeStatement = session.prepare(READ_RANGE_QUERY).setConsistencyLevel(readConsistency);
        this.readChunkStatement = session.prepare(READ_CHUNK_QUERY).setConsistencyLevel(readConsistency);
    }

    CompletableFuture<Void> delete(IRI id) {
        return executeWrite(deleteStatement.bind(id));
    }

    CompletableFuture<Void> insert(IRI id, Long size, int chunkIndex, InputStream chunk) {
        return executeWrite(insertStatement.bind(id, size, chunkIndex, chunk));
    }

    InputStream read(IRI id) {
        return retrieve(id, readStatement.bind(id));
    }

    InputStream readRange(IRI id, Integer first, Integer last) {
        return retrieve(id, readRangeStatement.bind(id, first, last));
    }

    CompletableFuture<ResultSet> get(IRI id) {
        return executeRead(retrieveStatement.bind(id));
    }

    //@formatter:off
    private InputStream retrieve(IRI id, Statement statement) {
        return stream(executeSyncRead(statement).spliterator(), false)
                        .map(r -> r.getInt("chunk_index"))
                        .peek(chunkNumber -> log.debug("Found record of chunk: {}", chunkNumber))
                        .map(chunkNumber -> readChunkStatement.bind(id, chunkNumber))
                        .<InputStream> map(s -> new LazyChunkInputStream(session, s))
                        .reduce(SequenceInputStream::new) // chunks now in one large stream
                        .orElseThrow(() -> new RuntimeTrellisException("Binary not found under IRI: " + id.getIRIString()));
    }
    //@formatter:on
}