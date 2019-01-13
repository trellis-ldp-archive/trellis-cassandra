package edu.si.trellis.query.binary;

import static java.util.stream.StreamSupport.stream;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import edu.si.trellis.LazyChunkInputStream;
import edu.si.trellis.query.CassandraQuery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.RuntimeTrellisException;

/**
 * A query that reads binary data from Cassandra.
 */
abstract class BinaryReadQuery extends CassandraQuery {

    private static final Logger log = getLogger(BinaryReadQuery.class);

    private static final String READ_CHUNK_QUERY = "SELECT chunk FROM " + BINARY_TABLENAME
                    + " WHERE identifier = :identifier and chunkIndex = :chunkIndex;";

    private final PreparedStatement readChunkStatement;

    public BinaryReadQuery(Session session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
        this.readChunkStatement = session.prepare(READ_CHUNK_QUERY);
    }

    //@formatter:off
    protected InputStream retrieve(IRI id, Statement statement) {
        return stream(executeSyncRead(statement).spliterator(), false)
                        .mapToInt(r -> r.getInt("chunkIndex"))
                        .peek(chunkIndex -> log.debug("Found record of chunk: {}", chunkIndex))
                        .mapToObj(chunkIndex -> readChunkStatement.bind()
                                            .setInt("chunkIndex", chunkIndex)
                                            .set("identifier",id, IRI.class))
                        .<InputStream> map(s -> new LazyChunkInputStream(session, s))
                        .reduce(SequenceInputStream::new) // chunks now in one large stream
                        .orElseThrow(() -> new RuntimeTrellisException("Binary not found under IRI: " + id.getIRIString()));
    }
    //@formatter:on

    /**
     * An {@link InputStream} that sequentially streams two underlying streams. {@link #skip(long)} calls {@code skip}
     * on the underlying streams before defaulting to using {@link IOUtils#skip(InputStream, long)}, and
     * {@link #read(byte[], int, int)} also calls {@code read(byte[], int, int)} on the underlying streams. This is
     * useful in particular with {@link ByteArrayInputStream}s, which have very fast
     * {@link ByteArrayInputStream#skip(long)} and {@link ByteArrayInputStream#read(byte[], int, int)} implementations.
     *
     */
    static class SequenceInputStream extends InputStream {

        private final InputStream s1, s2;

        /**
         * Changes from {@link #s1} to {@link #s2} to {@code null} via {@link #next()}.
         */
        private InputStream current;

        public SequenceInputStream(InputStream s1, InputStream s2) {
            this.current = (this.s1 = s1);
            this.s2 = s2;
        }

        @Override
        public long skip(long n) throws IOException {
            if (current == null || n <= 0) return 0;
            long toSkip = n;
            toSkip -= current.skip(toSkip);
            if (toSkip > 0) { // we ran out of bytes to skip from current
                toSkip -= IOUtils.skip(current, toSkip); // read them instead
                if (toSkip > 0) {
                    next();
                    toSkip -= skip(toSkip);
                }
            }
            return n - toSkip;
        }

        @Override
        public int read() throws IOException {
            if (current == null) return -1;
            int take = current.read();
            if (take == -1) {
                next();
                return read();
            }
            return take;
        }

        @Override
        public int read(byte b[], int offset, int length) throws IOException {
            if (offset < 0 || length < 0 || length > b.length - offset) throw new IndexOutOfBoundsException();
            if (length == 0) return 0;
            if (current == null) return -1;
            int read = current.read(b, offset, length);
            if (read <= 0) { // we couldn't get any bytes from current
                next();
                return read(b, offset, length);
            }
            return read;
        }

        private void next() throws IOException {
            if (current != null) current.close();
            current = current == s1 ? s2 : null;
        }
    }
}
