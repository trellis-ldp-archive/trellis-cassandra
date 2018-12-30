package edu.si.trellis;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.commons.codec.digest.DigestUtils.updateDigest;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.*;
import static org.slf4j.LoggerFactory.getLogger;

import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.*;

/**
 * Implements {@link BinaryService} by chunking binary data across Cassandra.
 *
 */
public class CassandraBinaryService implements BinaryService {

    private static final Logger log = getLogger(CassandraBinaryService.class);

    @SuppressWarnings("boxing")
    private static final CompletableFuture<Long> DONE = completedFuture(-1L);

    private static final String SHA = "SHA";

    // TODO JDK9 supports SHA3 algorithms (SHA3_256, SHA3_384, SHA3_512)
    private static final Set<String> algorithms = ImmutableSet.of(MD5, MD2, SHA, SHA_1, SHA_256, SHA_384, SHA_512);

    // package-private for testing
    static final String CASSANDRA_CHUNK_HEADER_NAME = "Cassandra-Chunk-Size";

    private final BinaryQueryContext cassandra;

    private final IdentifierService idService;

    private final int defaultChunkLength;

    /**
     * @param idService {@link IdentifierService} to use for binaries
     * @param chunkLength the maximum size of any chunk in this service
     * @param queryContext the Cassandra context for queries
     */
    @Inject
    public CassandraBinaryService(IdentifierService idService, @DefaultChunkSize int chunkLength,
                    BinaryQueryContext queryContext) {
        this.idService = idService;
        this.defaultChunkLength = chunkLength;
        log.info("Using configured default chunk length: {}", chunkLength);
        this.cassandra = queryContext;
    }

    @Override
    public CompletableFuture<Binary> get(IRI id) {
        log.debug("Retrieving binary content from: {}", id);
        return cassandra.get(id).thenApply(
                        rows -> requireNonNull(rows.one(), () -> "Binary not found under IRI: " + id.getIRIString()))
                        .thenApply(r -> new CassandraBinary(id, cassandra, r.getInt("chunkSize")));
    }

    @Override
    public CompletableFuture<Void> setContent(BinaryMetadata meta, InputStream stream) {
        log.debug("Recording binary content under: {}", meta.getIdentifier());
        final int chunkSize;
        if (meta.getHints() == null) chunkSize = defaultChunkLength;
        else {
            List<String> headers = meta.getHints().get(CASSANDRA_CHUNK_HEADER_NAME);
            if (headers == null) chunkSize = defaultChunkLength;
            else if (headers.size() > 1)
                throw new RuntimeTrellisException("Too many " + CASSANDRA_CHUNK_HEADER_NAME + " headers!");
            else chunkSize = Integer.parseInt(headers.get(0));
        }
        return setChunk(meta, stream, new AtomicInteger(), chunkSize)
                        .thenAccept(l -> log.debug("Recorded binary content under: {}", meta.getIdentifier()));
    }

    @SuppressWarnings("resource")
    private CompletableFuture<Long> setChunk(BinaryMetadata meta, InputStream data, AtomicInteger chunkIndex,
                    int chunkLength) {
        IRI id = meta.getIdentifier();
        log.debug("Recording chunk {} of binary content under: {}", chunkIndex.get(), id);

        try (NoopCloseCountingInputStream countingChunk = new NoopCloseCountingInputStream(
                        new BoundedInputStream(data, chunkLength))) {
            @SuppressWarnings("cast")
            // upcast to match this object with InputStreamCodec
            InputStream chunk = (InputStream) countingChunk;
            return cassandra.insert(id, chunkLength, chunkIndex.getAndIncrement(), chunk)
                            .thenApply(x -> countingChunk.getByteCount())
                            .thenComposeAsync(bytesStored -> bytesStored == chunkLength
                                            ? setChunk(meta, data, chunkIndex, chunkLength)
                                            : DONE, cassandra.writeWorkers);
        }
    }

    @Override
    public CompletableFuture<Void> purgeContent(IRI identifier) {
        return cassandra.delete(identifier);
    }

    @Override
    public CompletableFuture<MessageDigest> calculateDigest(IRI identifier, MessageDigest algorithm) {
        return get(identifier).thenApply(Binary::getContent).thenApplyAsync(in -> {
            try (InputStream stream = in) {
                return updateDigest(algorithm, stream);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }, cassandra.readWorkers);
    }

    @Override
    public Set<String> supportedAlgorithms() {
        return algorithms;
    }

    @Override
    public String generateIdentifier() {
        return idService.getSupplier().get();
    }
}
