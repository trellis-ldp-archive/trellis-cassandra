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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
        RollingInputStream assembledStream = new RollingInputStream(buffers);
        CompletableFuture<MappedAsyncPagingIterable<ByteBuffer>> thenApply = response
                        .thenApply(results -> results.map(row -> row.getByteBuffer("chunk")));
        CompletableFuture<MappedAsyncPagingIterable<ByteBuffer>> compose = thenApply
                        .thenCompose(page -> recurseThroughPages(buffers, page));
        CompletableFuture<Void> loadBuffers = compose.thenRun(assembledStream::finishRolling);
        assembledStream.finisher(loadBuffers);
        return assembledStream;
    }

    private CompletionStage<MappedAsyncPagingIterable<ByteBuffer>> recurseThroughPages(
                    TransferQueue<ByteBuffer> buffers, MappedAsyncPagingIterable<ByteBuffer> results) {
        log.trace("entering recurseThroughPages()");
        handleOnePage(buffers, results); // head
        if (results.hasMorePages()) // tail
            return results.fetchNextPage().thenCompose(nextPage -> recurseThroughPages(buffers, nextPage));
        return CompletableFuture.completedFuture(results);
    }

    private void handleOnePage(TransferQueue<ByteBuffer> buffers, MappedAsyncPagingIterable<ByteBuffer> results) {
        log.trace("Entering handleOnePage");
        results.currentPage().forEach(chunk -> uninterruptably(() -> {
            buffers.transfer(chunk);
            log.trace("transferred one chunk");
            return null;
        }));
        log.trace("Exiting handleOnePage");
    }

    private interface Interruptible<T> {
        /**
         * @return whether the operation succeeded
         * @throws InterruptedException
         */
        T run() throws InterruptedException;
    }

    private static <S> S uninterruptably(Interruptible<S> task) {
        boolean interrupted = false;
        try {
            while (true)
                try {
                    return task.run();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
        } finally {
            if (interrupted) currentThread().interrupt();
        }
    }

    private static class RollingInputStream extends InputStream {

        private static final int DONE = -1;

        private static final ByteBuffer INITIAL = ByteBuffer.allocate(0);

        private TransferQueue<ByteBuffer> buffers;

        /**
         * Used in {@link #close()}.
         */
        private CompletableFuture<?> finisher;

        private RollingInputStream(TransferQueue<ByteBuffer> buffers) {
            this.buffers = buffers;
            this.current = INITIAL;
        }

        void finisher(CompletableFuture<?> c) {
            this.finisher = c;
        }

        private volatile ByteBuffer current;

        private volatile boolean closed, finished;

        /**
         * Blocks until another buffer is available.
         * 
         * @return a buffer with fresh data
         */
        private void next() {
            log.trace("Entering next()");
            if (closed) return;
            uninterruptably(() -> {
                ByteBuffer next = null;
                // while we still haven't received a new buffer we keep checking
                while ((next = buffers.poll(1, SECONDS)) == null) {
                    log.trace("Entering next() loop");
                    if (closed || finished) { 
                        log.trace("Closed, leaving current alone");
                        return null;
                        }
                    log.trace("we are not closed");
                }
                log.trace("polled fresh buffer, setting current to  it");
                current = next;
                return null;
            });
        }

        private void ensureCurrent() {

            log.trace("entering ensureCurrent()");
            if (finished && !current.hasRemaining()) close();
            if (current == INITIAL) next();
        }

        @Override
        public long skip(long n) {
            ensureCurrent();
            if (closed) return 0;
            int skip = (int) Math.min(n, current.remaining());
            current.position(current.position() + skip);
            if (skip < n) {
                if (finished) {
                    close();
                    return skip;
                }
                ByteBuffer last = current;
                next();
                // no more fresh buffers
                if (last == current) return skip;
                // there are fresh buffers
                return skip + skip(n - skip);
            }
            return skip;
        }

        @Override
        public int read(byte[] b, int offset, int length) {
            log.trace("Entering read(byte[] b, int {}, int {})", offset, length);
            ensureCurrent();
            if (closed) return DONE;
            if (length == 0) { return 0; }
            if (length <= current.remaining()) {
                // we have enough in our current buffer
                current.get(b, offset, length);
                return length;
            }
            // we do not have enough in our current buffer
            if (finished) {
                // there will be no more bytes
                int available = current.remaining();
                if (available == 0) return DONE;
                current.get(b, offset, available);
                // we just used up all our bytes
                close();
                return available;
            }
            // how many bytes are left in current
            int available = current.remaining();
            // how many more bytes we will need after current is changed
            int toGo = length - available;
            // get the bytes we can from current
            current.get(b, offset, available);
            // how many bytes we have gotten
            int transferred = available;
            // if there is another buffer, read from it too
            ByteBuffer last = current;
            next();
            // if no more fresh buffers
            if (last == current) return transferred;
            // otherwise there are fresh buffers, so recurse
            int nextRead = read(b, offset + transferred, toGo);
            // if we successfully read from fresh buffer(s) record how many bytes
            if (nextRead != -1) transferred += nextRead;
            return transferred == 0 ? DONE : transferred;
        }

        /**
         * Indicates that no further data will be rolled.
         */
        void finishRolling() {
            log.trace("entering finishRolling()");
            finished = true;
        }

        @Override
        public void close() {
            log.trace("entering close()");
            closed = true;
            finisher.cancel(true);
            buffers.clear();
        }

        @Override
        public int read() {
            ensureCurrent();
            if (closed) return DONE;
            if (current.hasRemaining()) return Byte.toUnsignedInt(current.get());
            if (finished) {
                // no more bytes
                close();
                return DONE;
            }
            ByteBuffer last = current;
            next();
            // no fresh buffers
            if (last == current) return DONE;
            // there are fresh buffers, so recurse
            return read();
        }
    }
}
