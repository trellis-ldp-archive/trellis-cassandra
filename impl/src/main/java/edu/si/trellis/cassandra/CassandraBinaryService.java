package edu.si.trellis.cassandra;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static java.util.Base64.getEncoder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.codec.digest.DigestUtils.getDigest;
import static org.apache.commons.codec.digest.DigestUtils.updateDigest;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.*;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;

import com.datastax.driver.core.*;
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
import org.trellisldp.api.Binary;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.BinaryService;
import org.trellisldp.api.IdentifierService;

/**
 * Implements {@link BinaryService} by chunking binary data across Cassandra.
 *
 */
public class CassandraBinaryService extends CassandraService implements BinaryService {

    private static final Logger log = getLogger(CassandraBinaryService.class);

    private static final long DONE = -1;

    private static final String SHA = "SHA";

    // TODO JDK9 supports SHA3 algorithms (SHA3_256, SHA3_384, SHA3_512)
    // TODO Move digest calculation to the C* node.
    private static final Set<String> algorithms = ImmutableSet.of(MD5, MD2, SHA, SHA_1, SHA_256, SHA_384, SHA_512);

    private static final String INSERT_QUERY = "INSERT INTO Binarydata (identifier, chunk_index, chunk) VALUES (:identifier, :chunk_index, :chunk)";

    private static final String DELETE_QUERY = "DELETE FROM Binarydata WHERE identifier = ?;";

    private PreparedStatement deleteStatement, insertStatement;

    private final BinaryQueries queries;
    private final IdentifierService idService;

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
        log.info("Using chunk length: {}", chunkLength);
        this.queries = new BinaryQueries(session(), chunkLength);
    }

    @PostConstruct
    void initializeStatements() {
        this.insertStatement = session().prepare(INSERT_QUERY);
        this.deleteStatement = session().prepare(DELETE_QUERY);
    }

    @Override
    public CompletableFuture<Binary> get(IRI id) {
        return translate(session().executeAsync((Statement) null)).thenApply(rows -> {
            final Row meta = rows.one();
            boolean wasFound = meta != null;
            log.debug("Binary {} was {}found", id, wasFound ? "" : "not ");
            if (!wasFound) throw new RuntimeException();
            return meta;
        }).thenApply(r -> r.getLong("size")).thenApply(size -> new CassandraBinary(id, size, queries));
    }

    @Override
    public CompletableFuture<Void> setContent(BinaryMetadata meta, InputStream stream) {
        return setChunk(meta.getIdentifier(), stream, new AtomicInteger()).thenAccept(Long::longValue);
    }

    @SuppressWarnings("resource")
    private CompletableFuture<Long> setChunk(IRI iri, InputStream stream, AtomicInteger chunkIndex) {
        try (CountingInputStream countingChunk = new CountingInputStream(
                        new BoundedInputStream(stream, queries.maxChunkLength()))) {
            @SuppressWarnings("cast")
            // cast to match this object with InputStreamCodec
            InputStream chunk = (InputStream) countingChunk;
            Statement boundStatement = insertStatement.bind(iri, chunkIndex.getAndIncrement(), chunk)
                            .setConsistencyLevel(LOCAL_QUORUM);
            return translate(session().executeAsync(boundStatement.setConsistencyLevel(writeConsistency())))
                            .thenApply(r -> countingChunk.getByteCount())
                            .thenComposeAsync(bytesStored -> bytesStored == queries.maxChunkLength()
                                            ? setChunk(iri, stream, chunkIndex)
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
    public CompletableFuture<String> calculateDigest(IRI identifier, String algorithm) {
        MessageDigest digest = getDigest(algorithm);
        return getContent(identifier).thenApplyAsync(in -> {
            try (InputStream stream = in) {
                return getEncoder().encodeToString(updateDigest(digest, stream).digest());
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

    static class BinaryQueries {

        private final Session session;

        private static final String READ_QUERY = "SELECT chunk, chunk_index FROM Binarydata WHERE identifier = ?;";

        private static final String READ_RANGE_QUERY = "SELECT chunk, chunk_index FROM Binarydata WHERE identifier = ? and chunk_index >= :start and chunk_index <= :end;";

        private PreparedStatement readRangeStatement, readStatement;

        private final int maxChunkLength;

        private final ConsistencyLevel readConsistency;

        public BinaryQueries(Session session, int maxChunkLength, ConsistencyLevel consistency) {
            this.session = session;
            this.readStatement = session.prepare(READ_QUERY);
            this.readRangeStatement = session.prepare(READ_RANGE_QUERY);
            this.maxChunkLength = maxChunkLength;
            this.readConsistency = consistency;
        }

        int maxChunkLength() {
            return maxChunkLength;
        }

        Session session() {
            return session;
        }

        PreparedStatement readRangeStatement() {
            return readRangeStatement;
        }

        PreparedStatement readStatement() {
            return readStatement;
        }

        ConsistencyLevel readConsistency() {
            return readConsistency;
        }

    }
}
