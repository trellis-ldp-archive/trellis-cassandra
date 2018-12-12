package edu.si.trellis.cassandra;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.commons.codec.digest.DigestUtils.updateDigest;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.*;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
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
public class CassandraBinaryService extends CassandraService implements BinaryService {

    private static final Logger log = getLogger(CassandraBinaryService.class);

    @SuppressWarnings("boxing")
    private static final Long DONE = -1L;

    private static final String SHA = "SHA";

    // TODO JDK9 supports SHA3 algorithms (SHA3_256, SHA3_384, SHA3_512)
    private static final Set<String> algorithms = ImmutableSet.of(MD5, MD2, SHA, SHA_1, SHA_256, SHA_384, SHA_512);

    private final BinaryQueryContext queryContext;

    private final IdentifierService idService;

    private final int maxChunkLength;

    /**
     * @param idService {@link IdentifierService} to use for binaries
     * @param chunkLength the maximum size of any chunk in this service
     * @param queryContext the Cassandra context for queries
     */
    @Inject
    public CassandraBinaryService(IdentifierService idService, @MaxChunkSize int chunkLength,
                    BinaryQueryContext queryContext) {
        this.idService = idService;
        this.maxChunkLength = chunkLength;
        log.info("Using configured chunk length: {}", chunkLength);
        this.queryContext = queryContext;
    }

    @Override
    public CompletableFuture<Binary> get(IRI id) {
        log.debug("Retrieving binary content from: {}", id);
        return queryContext.get(id).thenApply(rows -> {
            final Row meta = rows.one();
            boolean wasFound = meta != null;
            log.debug("Binary {} was {}found", id, wasFound ? "" : "not ");
            if (!wasFound) throw new RuntimeTrellisException("Binary not found under IRI: " + id.getIRIString());
            return meta;
        }).thenApply(r -> r.getLong("size"))
                        .thenApply(size -> new CassandraBinary(id, size, queryContext, maxChunkLength));
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
            return queryContext.insert(id, size, chunkIndex.getAndIncrement(), chunk)
                            .thenApply(r -> countingChunk.getByteCount())
                            .thenComposeAsync(bytesStored -> bytesStored == maxChunkLength
                                            ? setChunk(meta, stream, chunkIndex)
                                            : finished(id), queryContext.workers);
        }
    }

    private static CompletableFuture<Long> finished(IRI id) {
        return supplyAsync(() -> {
            log.debug("Finished persisting: {}", id);
            return DONE;
        }, Runnable::run);
    }

    @Override
    public CompletableFuture<Void> purgeContent(IRI identifier) {
        return queryContext.delete(identifier);
    }

    @Override
    public CompletableFuture<byte[]> calculateDigest(IRI identifier, MessageDigest algorithm) {
        return get(identifier).thenApply(Binary::getContent).thenApplyAsync(in -> {
            try (InputStream stream = in) {
                return updateDigest(algorithm, stream).digest();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }, queryContext.workers);
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
