package edu.si.trellis.cassandra;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.commons.codec.digest.DigestUtils.updateDigest;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.*;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.*;

/**
 * Implements {@link BinaryService} by chunking binary data across Cassandra.
 *
 */
public class CassandraBinaryService extends CassandraService implements BinaryService {

    private static final Logger log = getLogger(CassandraBinaryService.class);

    @SuppressWarnings("boxing")
    private static final Long DONE = -1L;

    private static final String SHA = "SHA";

    // TODO JDK9 supports SHA3 algorithms (SHA3_256, SHA3_384, SHA3_512)
    // TODO Move digest calculation to the C* node.
    private static final Set<String> algorithms = ImmutableSet.of(MD5, MD2, SHA, SHA_1, SHA_256, SHA_384, SHA_512);

    private static final String INSERT_QUERY = "INSERT INTO Binarydata (identifier, size, chunk_index, chunk) VALUES (:identifier, :size, :chunk_index, :chunk)";

    private static final String RETRIEVE_QUERY = "SELECT size FROM Binarydata WHERE identifier = ? LIMIT 1;";

    private static final String DELETE_QUERY = "DELETE FROM Binarydata WHERE identifier = ?;";

    private PreparedStatement deleteStatement, insertStatement, retrieveStatement;

    private final BinaryQueryContext queries;

    private final IdentifierService idService;

    private final int maxChunkLength;

    /**
     * @param idService {@link IdentifierService} to use for binaries
     * @param session {@link Session} to use to connect to Cassandra
     * @param chunkLength the maximum size of any chunk in this service
     * @param readCons the read-consistency to use
     * @param writeCons the write-consistency to use
     */
    @Inject
    public CassandraBinaryService(IdentifierService idService, Session session, @MaxChunkSize int chunkLength,
                    @BinaryReadConsistency ConsistencyLevel readCons,
                    @BinaryWriteConsistency ConsistencyLevel writeCons) {
        super(session, readCons, writeCons);
        this.idService = idService;
        this.maxChunkLength = chunkLength;
        log.info("Using chunk length: {}", chunkLength);
        this.queries = new BinaryQueryContext(session(), readConsistency());
    }

    @PostConstruct
    void initializeStatements() {
        this.insertStatement = session().prepare(INSERT_QUERY);
        this.deleteStatement = session().prepare(DELETE_QUERY);
        this.retrieveStatement = session().prepare(RETRIEVE_QUERY);
    }

    @Override
    public CompletableFuture<Binary> get(IRI id) {
        log.debug("Retrieving binary content from: {}", id);
        return translate(session().executeAsync(retrieveStatement.bind(id))).thenApply(rows -> {
            final Row meta = rows.one();
            boolean wasFound = meta != null;
            log.debug("Binary {} was {}found", id, wasFound ? "" : "not ");
            if (!wasFound) throw new RuntimeTrellisException("Binary not found under IRI: " + id.getIRIString());
            return meta;
        }).thenApply(r -> r.getLong("size")).thenApply(size -> new CassandraBinary(id, size, queries, maxChunkLength));
    }

    @Override
    public CompletableFuture<Void> setContent(BinaryMetadata meta, InputStream stream) {
        log.debug("Recording binary content under: {}", meta.getIdentifier());
        return setChunk(meta, stream, new AtomicInteger()).thenAccept(Long::longValue);
    }

    @SuppressWarnings("resource")
    private CompletableFuture<Long> setChunk(BinaryMetadata meta, InputStream stream, AtomicInteger chunkIndex) {
        IRI id = meta.getIdentifier();
        Long size = meta.getSize().orElse(null);
        log.debug("Recording chunk {} of binary content under: {}", chunkIndex.get(), id);

        try (CountingInputStream countingChunk = new CountingInputStream(
                        new BoundedInputStream(stream, maxChunkLength))) {
            @SuppressWarnings("cast")
            // upcast to match this object with InputStreamCodec
            InputStream chunk = (InputStream) countingChunk;
            Statement boundStatement = insertStatement.bind(id, size, chunkIndex.getAndIncrement(), chunk)
                            .setConsistencyLevel(LOCAL_QUORUM);
            return translate(session().executeAsync(boundStatement.setConsistencyLevel(writeConsistency())))
                            .thenApply(r -> countingChunk.getByteCount())
                            .thenComposeAsync(bytesStored -> bytesStored == maxChunkLength
                                            ? setChunk(meta, stream, chunkIndex)
                                            : completedFuture(DONE), mappingThread);
        }
    }

    @Override
    public CompletableFuture<Void> purgeContent(IRI identifier) {
        Statement boundStatement = deleteStatement.bind(identifier.getIRIString())
                        .setConsistencyLevel(writeConsistency());
        return translate(session().executeAsync(boundStatement)).thenAccept(x -> {});
    }

    @Override
    public CompletableFuture<byte[]> calculateDigest(IRI identifier, MessageDigest algorithm) {
        return get(identifier).thenApply(Binary::getContent).thenApplyAsync(in -> {
            try (InputStream stream = in) {
                return updateDigest(algorithm, stream).digest();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }, mappingThread);
    }

    @Override
    public Set<String> supportedAlgorithms() {
        return algorithms;
    }

    @Override
    public String generateIdentifier() {
        return idService.getSupplier().get();
    }

    /**
     * TODO threadpool?
     */
    private Executor mappingThread = Runnable::run;

    static class BinaryQueryContext {

        private final Session session;

        private static final String READ_QUERY = "SELECT chunk_index FROM Binarydata WHERE identifier = ?;";

        private static final String READ_RANGE_QUERY = "SELECT chunk_index FROM Binarydata WHERE identifier = ? and chunk_index >= :start and chunk_index <= :end;";

        private static final String READ_CHUNK_QUERY = "SELECT chunk FROM Binarydata WHERE identifier = ? and chunk_index = :chunk_index;";

        private PreparedStatement readRangeStatement, readStatement, readChunkStatement;

        public BinaryQueryContext(Session session, ConsistencyLevel consistency) {
            this.session = session;
            this.readStatement = session.prepare(READ_QUERY).setConsistencyLevel(consistency);
            this.readRangeStatement = session.prepare(READ_RANGE_QUERY).setConsistencyLevel(consistency);
            this.readChunkStatement = session.prepare(READ_CHUNK_QUERY).setConsistencyLevel(consistency);
        }

        Session session() {
            return session;
        }

        PreparedStatement readRangeStatement() {
            return readRangeStatement;
        }

        PreparedStatement readSingleChunk() {
            return readChunkStatement;
        }

        PreparedStatement readStatement() {
            return readStatement;
        }
    }
}
