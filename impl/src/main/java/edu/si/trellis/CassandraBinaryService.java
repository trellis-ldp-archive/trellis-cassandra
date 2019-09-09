package edu.si.trellis;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import edu.si.trellis.query.binary.Delete;
import edu.si.trellis.query.binary.GetChunkSize;
import edu.si.trellis.query.binary.Insert;
import edu.si.trellis.query.binary.Read;
import edu.si.trellis.query.binary.ReadRange;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.BinaryService;
import org.trellisldp.api.IdentifierService;
import org.trellisldp.api.RuntimeTrellisException;

/**
 * Implements {@link BinaryService} by chunking binary data across Cassandra.
 *
 */
public class CassandraBinaryService implements BinaryService {

    private static final Logger log = getLogger(CassandraBinaryService.class);

    @SuppressWarnings("boxing")
    private static final CompletableFuture<Long> DONE = completedFuture(-1L);

    private final Executor writeWorkers = newCachedThreadPool();

    // package-private for testing
    static final String CASSANDRA_CHUNK_HEADER_NAME = "Cassandra-Chunk-Size";

    private final IdentifierService idService;

    private final int defaultChunkLength;

    private final GetChunkSize get;

    private final Insert insert;

    private final Delete delete;

    private final Read read;

    private final ReadRange readRange;

    /**
     * @param idService {@link IdentifierService} to use for binaries
     * @param chunkLength the maximum size of any chunk in this service
     * @param get a {@link GetChunkSize} query to use
     * @param insert a {@link Insert} query to use
     * @param delete a {@link Delete} query to use
     * @param read a {@link Read} query to use
     * @param readRange a {@link ReadRange} query to use
     */
    @Inject
    public CassandraBinaryService(IdentifierService idService, @DefaultChunkSize int chunkLength, GetChunkSize get,
                    Insert insert, Delete delete, Read read, ReadRange readRange) {
        this.idService = idService;
        this.defaultChunkLength = chunkLength;
        log.info("Using configured default chunk length: {}", chunkLength);
        this.get = get;
        this.insert = insert;
        this.delete = delete;
        this.read = read;
        this.readRange = readRange;
    }

    @Override
    public CompletionStage<Binary> get(IRI id) {
        log.trace("Retrieving binary content from: {}", id);
        return get.execute(id).thenApply(row -> row.getInt("chunkSize"))
                        .thenApply(chunkSize -> new CassandraBinary(id, read, readRange, chunkSize));
    }

    @Override
    public CompletionStage<Void> setContent(BinaryMetadata meta, InputStream stream) {
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
    private CompletionStage<Long> setChunk(BinaryMetadata meta, InputStream data, AtomicInteger chunkIndex,
                    int chunkLength) {
        IRI id = meta.getIdentifier();
        log.debug("Recording chunk {} of binary content under: {}", chunkIndex.get(), id);

        try (NoopCloseCountingInputStream countingChunk = new NoopCloseCountingInputStream(
                        new BoundedInputStream(data, chunkLength))) {
            @SuppressWarnings("cast")
            // upcast to match this object with InputStreamCodec
            InputStream chunk = (InputStream) countingChunk;
            return insert.execute(id, chunkLength, chunkIndex.getAndIncrement(), chunk)
                            .thenComposeAsync(dummy -> countingChunk.getByteCount() == chunkLength
                                            ? setChunk(meta, data, chunkIndex, chunkLength)
                                            : DONE, writeWorkers);
        }
    }

    @Override
    public CompletionStage<Void> purgeContent(IRI identifier) {
        return delete.execute(identifier);
    }

    @Override
    public String generateIdentifier() {
        return idService.getSupplier().get();
    }
}
