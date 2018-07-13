package edu.si.trellis.cassandra;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static java.lang.Math.floorDiv;
import static java.util.Arrays.copyOf;
import static java.util.Base64.getEncoder;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.Range;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.BinaryService;
import org.trellisldp.api.IdentifierService;
import org.trellisldp.api.RuntimeTrellisException;

/**
 * Implements {@link BinaryService} by chunking binary data across Cassandra.
 *
 */
public class CassandraBinaryService implements BinaryService {
    private static final Logger log = LoggerFactory.getLogger(CassandraBinaryService.class);

    private final Session cassandraSession;

    private int chunkLength = 1 * 1024 * 1024;

    private static final String SHA = "SHA";

    // TODO JDK9 supports SHA3 algorithms (SHA3_256, SHA3_384, SHA3_512)
    // TODO Move digest calculation to the C* node.
    private static final Set<String> algorithms = ImmutableSet.of(MD5, MD2, SHA, SHA_1, SHA_256, SHA_384, SHA_512);

    private static final String INSERT_QUERY = "INSERT INTO Binarydata (identifier, chunk_index, chunk) VALUES (:identifier, :chunk_index, :chunk)";

    private final PreparedStatement insertStatement;

    private static final String CONTAINS_QUERY = "SELECT identifier FROM Binarydata WHERE identifier = ? and chunk_index = 0;";

    private final PreparedStatement containsStatement;

    private static final String READ_QUERY = "SELECT chunk FROM Binarydata WHERE identifier = ?;";

    private final PreparedStatement readStatement;

    private static final String READ_RANGE_QUERY = "SELECT chunk, chunk_index FROM Binarydata WHERE identifier = ? and chunk_index >= :start and chunk_index <= :end;";

    private final PreparedStatement readRangeStatement;

    private static final String DELETE_QUERY = "DELETE FROM Binarydata WHERE identifier = ?;";

    private final PreparedStatement deleteStatement;

    private final IdentifierService idService;

    public CassandraBinaryService(IdentifierService idService, Session session, int chunkLength) {
        this.idService = idService;
        this.cassandraSession = session;
        this.chunkLength = chunkLength;
        insertStatement = session.prepare(INSERT_QUERY);
        containsStatement = session.prepare(CONTAINS_QUERY);
        readStatement = session.prepare(READ_QUERY);
        readRangeStatement = session.prepare(READ_RANGE_QUERY);
        deleteStatement = session.prepare(DELETE_QUERY);
    }

    @Override
    public Optional<InputStream> getContent(IRI identifier, List<Range<Integer>> ranges) {
        requireNonNull(ranges, "Byte ranges may not be null!");
        if (ranges.isEmpty()) throw new IllegalArgumentException("ranges cannot be empty!");

        // TODO https://github.com/trellis-ldp/trellis/issues/148
        try {
            return Optional.of(readRanges(identifier, ranges).get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    private Executor translator = Runnable::run;

    private <T> CompletableFuture<T> translate(ListenableFuture<T> from) {
        return supplyAsync(() -> {
            try {
                return from.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeTrellisException(e);
            }
        }, translator);
    }

    @Override
    public Optional<InputStream> getContent(IRI identifier) {
        // TODO https://github.com/trellis-ldp/trellis/issues/148
        try {
            return Optional.of(readAll(identifier).get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    private CompletableFuture<InputStream> readAll(IRI identifier) {
        Statement boundStatement = readStatement.bind(identifier.getIRIString())
                        .setConsistencyLevel(LOCAL_ONE);
        ResultSetFuture results = cassandraSession.executeAsync(boundStatement);
        return translate(results).thenApply(resultSet -> stream(resultSet.spliterator(), false)
                        .map(r -> r.get("chunk", InputStream.class))
                        .reduce(SequenceInputStream::new).get());
    }

    private CompletableFuture<InputStream> readRanges(IRI identifier, List<Range<Integer>> ranges) {
        return ranges.stream().map(r -> readRange(identifier, r)).sequential()
                        .reduce((chunk1, chunk2) -> chunk1.thenCombine(chunk2, SequenceInputStream::new)).get();
    }

    private CompletableFuture<InputStream> readRange(IRI identifier, Range<Integer> range) {
        int startIndex = floorDiv(range.getMinimum(), chunkLength);
        int endIndex = floorDiv(range.getMaximum(), chunkLength);
        int chunkStreamStart = range.getMinimum() % chunkLength;
        int rangeSize = range.getMaximum() - range.getMinimum() + 1; // +1 because range is inclusive
        Statement boundStatement = readRangeStatement.bind(identifier.getIRIString(), startIndex, endIndex)
                        .setConsistencyLevel(LOCAL_ONE);
        ResultSetFuture results = cassandraSession.executeAsync(boundStatement);
        return translate(results).thenApply(resultSet -> stream(resultSet.spliterator(), false)
                        .peek(r -> { log.debug("Retrieving chunk: {}", r.getInt("chunk_index")); })
                        .map(r -> r.get("chunk", InputStream.class))
                        .reduce(SequenceInputStream::new).get())
                        .thenApply(in -> {
                            try {
                                in.skip(chunkStreamStart);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            return in;
                        })
                        .thenApply(in -> new BoundedInputStream(in, rangeSize));
    }

    @Override
    public Boolean exists(IRI identifier) {
        Statement boundStatement = containsStatement.bind(identifier.getIRIString())
                        .setConsistencyLevel(LOCAL_ONE);
        return cassandraSession.execute(boundStatement).one() != null;
    }

    @Override
    public void setContent(IRI identifier, InputStream stream, Map<String, String> metadata /* ignored */) {
        byte[] buffer = new byte[chunkLength];
        int chunkIndex = -1;
        try {
            for (int len = stream.read(buffer); len != -1; len = stream.read(buffer)) {
                chunkIndex++;
                try (ByteArrayInputStream chunk = new ByteArrayInputStream(copyOf(buffer, len))) {
                    Statement boundStatement = insertStatement.bind(identifier.getIRIString(), chunkIndex, chunk)
                                    .setConsistencyLevel(LOCAL_QUORUM);
                    cassandraSession.execute(boundStatement);
                }
            }
        } catch (final IOException ex) {
            log.error("Error while setting content: {}", ex.getMessage());
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void purgeContent(IRI identifier) {
        Statement boundStatement = deleteStatement.bind(identifier.getIRIString());
        cassandraSession.execute(boundStatement);
    }

    @Override
    public Optional<String> digest(final String algorithm, final InputStream stream) {
        return SHA.equals(algorithm) 
                        ? of(SHA_1).map(DigestUtils::getDigest).map(digest(stream))
                        : ofNullable(algorithm).filter(supportedAlgorithms()::contains).map(DigestUtils::getDigest)
                                        .map(digest(stream));
    }

    @Override
    public Set<String> supportedAlgorithms() {
        return algorithms;
    }

    private Function<MessageDigest, String> digest(final InputStream in) {
        return algorithm -> {
            try (InputStream stream = in) {
                return getEncoder().encodeToString(DigestUtils.updateDigest(algorithm, stream).digest());
            } catch (final IOException e) {
                log.error("Error computing digest!", e);
            }
            return "";
        };
    }

    @Override
    public String generateIdentifier() {
        return idService.getSupplier().get();
    }
}
