package edu.si.trellis.cassandra;
import static java.util.Base64.getEncoder;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD2;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_256;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_384;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_512;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.Range;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.BinaryService;
import org.trellisldp.api.IdentifierService;

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

    private static final String READ_RANGE_QUERY = "SELECT chunk FROM Binarydata WHERE identifier = ? and chunk_index >= :start and chunk_index <= :end;";

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
        requireNonNull(ranges, "Byte ranges may not be null");
        return ranges.isEmpty() ? Optional.ofNullable(readAll(identifier))
                        : ranges.stream().map(r -> readRange(identifier, r)).sequential()
                                        .reduce(SequenceInputStream::new);
    }              

    private InputStream readAll(IRI identifier) {
        final PipedOutputStream output = new PipedOutputStream();
        final PipedInputStream input;
        try {
            input = new PipedInputStream(output);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        BoundStatement boundStatement = readStatement.bind(identifier.getIRIString());
        boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        ResultSet results = cassandraSession.execute(boundStatement);
        new Thread(() -> {
            try {
                for (Row r : results) {
                    ByteBuffer bbuf = r.getBytes(0);
                    output.write(bbuf.array());
                    output.flush();
                }
                output.close();
            } catch (IOException e) {
                log.error("Error reading chunk into stream: {}", e);
                throw new UncheckedIOException("Error reading chunk into stream: {}", e);
            }
        }).start();
        return input;
    }

    private InputStream readRange(IRI identifier, Range<Integer> range) {
        PipedOutputStream output = new PipedOutputStream();
        final PipedInputStream input;
        try {
            input = new PipedInputStream(output);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int startIndex = Math.floorDiv(range.getMinimum(), chunkLength);
        int endIndex = Math.floorDiv(range.getMaximum(), chunkLength);
        int chunkStreamStart = range.getMinimum() % chunkLength;
        int rangeSize = range.getMaximum() - range.getMinimum() + 1; // +1 because range is inclusive
        int chunkStreamSize = chunkStreamStart + rangeSize;
        BoundStatement boundStatement = readRangeStatement.bind(identifier.getIRIString(), startIndex, endIndex);
        boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        ResultSet results = cassandraSession.execute(boundStatement);
        new Thread(() -> {
            try (OutputStream out = output) {
                for (Row r : results) {
                    ByteBuffer bbuf = r.getBytes(0);
                    out.write(bbuf.array());
                    out.flush();
                }
            } catch (IOException e) {
                log.error("Error reading chunk into stream: {} for readRange {}", e.getMessage(), range.toString());
                throw new UncheckedIOException("Error reading chunk into stream", e);
            }
        }).start();
        try (BoundedInputStream boundedInputStream = new BoundedInputStream(input, chunkStreamSize)) {
            boundedInputStream.setPropagateClose(false);
            for (int i = 0; i < chunkStreamStart; i++)
                boundedInputStream.read();
            return boundedInputStream;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Boolean exists(IRI identifier) {
        BoundStatement boundStatement = containsStatement.bind(identifier.getIRIString());
        containsStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        return cassandraSession.execute(boundStatement).one() != null;
    }

    @Override
    public void setContent(IRI identifier, InputStream stream, Map<String, String> metadata /* ignored */) {
        byte[] buffer = new byte[chunkLength];
        int chunkIndex = -1;
        try {
            for (int len = stream.read(buffer); len != -1; len = stream.read(buffer)) {
                chunkIndex++;
                ByteBuffer chunk = ByteBuffer.allocate(len).put(buffer, 0, len);
                chunk.flip();
                BoundStatement boundStatement = insertStatement.bind(identifier.getIRIString(), chunkIndex, chunk);
                boundStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
                this.cassandraSession.execute(boundStatement);
            }
        } catch (final IOException ex) {
            log.error("Error while setting content: {}", ex.getMessage());
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void purgeContent(IRI identifier) {
        BoundStatement boundStatement = deleteStatement.bind(identifier.getIRIString());
        cassandraSession.execute(boundStatement);
    }

    @Override
    public Optional<String> digest(final String algorithm, final InputStream stream) {
        if (SHA.equals(algorithm)) { return of(SHA_1).map(DigestUtils::getDigest).flatMap(digest(stream)); }
        return ofNullable(algorithm).filter(supportedAlgorithms()::contains).map(DigestUtils::getDigest)
                        .flatMap(digest(stream));
    }

    @Override
    public Set<String> supportedAlgorithms() {
        return algorithms;
    }

    private Function<MessageDigest, Optional<String>> digest(final InputStream stream) {
        return algorithm -> {
            try {
                final String digest = getEncoder().encodeToString(DigestUtils.updateDigest(algorithm, stream).digest());
                stream.close();
                return of(digest);
            } catch (final IOException ex) {
                log.error("Error computing digest", ex);
            }
            return empty();
        };
    }

    @Override
    public String generateIdentifier() {
        return idService.getSupplier().get();
    }

}
