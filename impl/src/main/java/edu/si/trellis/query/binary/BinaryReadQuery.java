package edu.si.trellis.query.binary;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.slf4j.Logger;

/**
 * A query that reads binary data from Cassandra.
 */
abstract class BinaryReadQuery extends BinaryQuery {

    private static final Logger log = getLogger(BinaryReadQuery.class);

    BinaryReadQuery(CqlSession session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
    }

    /**
     * @param statement a CQL query that retrieves the chunk indexes of chunks for {@code id}
     * @return A future for an {@link InputStream} of bytes as requested. The {@code skip} method of this
     *         {@code InputStream} is guaranteed to skip as many bytes as asked.
     */

    protected InputStream retrieve(BoundStatement statement) {
        TransferQueue<ByteBuffer> buffers = new LinkedTransferQueue<>();
        CompletableFuture<AsyncResultSet> response = executeRead(statement).toCompletableFuture();
        RollingInputStream assembledStream = new RollingInputStream(buffers, response);
        response
            .thenApply(results -> results.map(row -> row.getByteBuffer("chunk")))
            .thenAcceptAsync(page -> recurseThroughPages(buffers, page))
            .thenRun(assembledStream::finishRolling);
        return assembledStream;
    }

    private void recurseThroughPages(TransferQueue<ByteBuffer> buffers, MappedAsyncPagingIterable<ByteBuffer> results) {
        log.trace("Entering recurseThroughPages");
        handleOnePage(buffers, results); // head
        while (results.hasMorePages()) {
            log.trace("Entering loop inside recurseThroughPages");
            handleOnePage(buffers, results);
            results.fetchNextPage().thenAcceptAsync(nextPage -> recurseThroughPages(buffers, nextPage)); // tail
        }
    }

    private void handleOnePage(TransferQueue<ByteBuffer> buffers, MappedAsyncPagingIterable<ByteBuffer> results) {
        log.trace("Entering handleOnePage");
        results.currentPage().forEach(chunk ->
            uninterruptably(() -> {
                  buffers.transfer(chunk);
                  return true;
            }));
        log.trace("Exiting handleOnePage");
    }

    private interface Interruptible {
        /**
         * @return whether the operation succeeded
         * @throws InterruptedException
         */
        boolean run() throws InterruptedException;
    }

    private static boolean uninterruptably(Interruptible task) {
        boolean interrupted = false;
        try {
            while (!interrupted)
                try {
                    return task.run();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
        } finally {
            if (interrupted) currentThread().interrupt();
        }
        return false;
    }

    private static class RollingInputStream extends InputStream {

        private static final int DONE = -1;

        private TransferQueue<ByteBuffer> buffers;

        /**
         * Used in {@link #close()}.
         */
        private CompletableFuture<?> response;

        private RollingInputStream(TransferQueue<ByteBuffer> buffers, CompletableFuture<?> response) {
            this.buffers = buffers;
            this.response = response;
        }

        private ByteBuffer current;

        private volatile boolean closed, finishedRolling;

        /**
         * Blocks until another buffer is available.
         * 
         * @throws IOException
         */
        private boolean next() {
            log.trace("Entering next()");
            return uninterruptably(() -> {
                do {
                    log.trace("Entering loop in next()");
                    // in between checking to see if we have been closed
                    if (closed || finishedRolling) {
                        log.trace("Finished or closed, no more buffers allowed ");
                        return false;
                    }
                    // while we still haven't received a new buffer we keep checking
                } while ((current = buffers.poll(1, SECONDS)) == null);
                log.trace("Polled a new buffer for current");
                return true;
            });
        }

        private void ensureCurrent() {
            if (current == null) next();
        }

        @Override
        public long skip(long n) throws IOException {
            log.trace("Entering skip({})", n);
            if (closed) return 0;
            ensureCurrent();
            int skip = (int) Math.min(n, current.remaining());
            current.position(current.position() + skip);
            if (skip < n) {
                log.trace("skip < n {} < {}", skip, n);
                if (next()) return skip + skip(n - skip);
            }
            return skip;
        }

        @Override
        public int read(byte[] b, int offset, int length) throws IOException {
            log.trace("Calling read({}, {}, {})", "b", offset, length);
            if (closed || length == 0) return 0;
            ensureCurrent();
            
            log.trace("Current has {} remaining", current.remaining());
            
            if (length <= current.remaining()) {
                log.trace("We have enough in our current buffer");
                current.get(b, offset, length);
                return length;
            }
            
            log.trace("we don't have enough in our current buffer");
            // how many bytes are left in current
            int available = current.remaining();
            // how many bytes we will need after current is changed
            int toGo = length - available;
            // get the bytes we can from current
            current.get(b, offset, available);
            int transferred = available;
            log.trace("Got {} bytes, looking for {} more", transferred, toGo);
            // if there is another buffer, read from it too
            if (next()) {
                log.trace("Found a new current to get them from");
                int nextRead = read(b, offset + transferred, toGo);
                log.trace("Recursed to get {} more bytes", nextRead);
                if (nextRead != -1) {
                    transferred += nextRead;
                }
                log.trace("and we've read {}", transferred);
                return transferred;
            }
            log.trace("No more buffers, we cannot get the last {} bytes", toGo);
            if (transferred == 0) {
                log.trace("No more buffers and we got no more data from current");
                return -1;
            }
            return transferred;
        }

        /**
         * Indicates that no further data will be rolled.
         */
        public void finishRolling() {
            log.trace("Entering finishRolling()");
            finishedRolling = true;
        }

        @Override
        public void close() {
            log.trace("Entering close()");
            closed = true;
            response.cancel(true);
            buffers.clear();
        }

        @Override
        public int read() throws IOException {
            log.trace("Entering read()");
            if (closed) return DONE;
            log.trace("Not closed in read()");
            ensureCurrent();
            log.trace("Did find a current, asking for a byte");
            if (current.hasRemaining()) {
                log.trace("Current has a byte!");
                int unsignedInt = Byte.toUnsignedInt(current.get());
                log.trace("Found a byte: {}", unsignedInt);
                return unsignedInt;
            }
            log.trace("Didn't find any bytes, calling next()");
            if (next()) {
                log.trace("There was a next buffer, recursing into read()");
                return read();
            }
            log.trace("There was no next buffer  we are done");
            return DONE;

        }
    }
}
