package edu.si.trellis.query.binary;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query that reads binary data from Cassandra.
 */
abstract class BinaryReadQuery extends BinaryQuery {

    private static final Logger log = getLogger(BinaryReadQuery.class);
    
    private ExecutorService readWorkers = Executors.newCachedThreadPool();

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
        RollingByteBufferInputStream assembledStream = new RollingByteBufferInputStream(buffers);
        CompletableFuture<Void> loadBuffers = response
                                                .thenApplyAsync(results -> results.map(row -> row.getByteBuffer("chunk")), readWorkers)
                                                .thenCompose(page -> recurseThroughPages(buffers, page))
                                                .thenRun(assembledStream::finish);
        assembledStream.finishWith(loadBuffers);
        return assembledStream;
    }

    private CompletionStage<MappedAsyncPagingIterable<ByteBuffer>> recurseThroughPages(
                    TransferQueue<ByteBuffer> buffers, MappedAsyncPagingIterable<ByteBuffer> results) {
        handleOnePage(buffers, results); // head
        if (results.hasMorePages()) // tail
            return results.fetchNextPage()
                            .thenComposeAsync(nextPage -> recurseThroughPages(buffers, nextPage), readWorkers);
        return CompletableFuture.completedFuture(results);
    }

    private void handleOnePage(TransferQueue<ByteBuffer> buffers, MappedAsyncPagingIterable<ByteBuffer> results) {
        results.currentPage().forEach(chunk -> uninterruptably(() -> buffers.transfer(chunk)));
    }

    private interface Interruptible {
        /**
         * @throws InterruptedException
         */
        void run() throws InterruptedException;
    }

    private static void uninterruptably(Interruptible task) {
        boolean interrupted = false;
        try {
            while (true)
                try {
                    task.run();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
        } finally {
            if (interrupted) currentThread().interrupt();
        }
    }

    private static class RollingByteBufferInputStream extends InputStream {

        private static final int DONE = -1;

        private static final ByteBuffer INITIAL = ByteBuffer.allocate(0);

        /**
         * Ms to wait for a buffer from Cassandra before timing out.
         */
        private static final int BUFFER_TIMEOUT = 5000;

        private TransferQueue<ByteBuffer> buffers;

        /**
         * Used in {@link #close()} to hopefully cancel any upstream operations.
         */
        private CompletableFuture<?> finishWith;

        private RollingByteBufferInputStream(TransferQueue<ByteBuffer> buffers) {
            this.buffers = buffers;
            this.current = INITIAL;
        }

        /**
         * @param c a task that will eventually call {@link #finish}.
         */
        private void finishWith(CompletableFuture<?> c) {
            this.finishWith = c;
        }

        private volatile ByteBuffer current;

        private volatile boolean closed, finished;

        /**
         * Blocks until another buffer is available.
         */
        private void next() {
            if (closed) return;
            AtomicBoolean timedout = new AtomicBoolean();
            Timer timer = new Timer();
            TimerTask task = new TimerTask() {

                @Override
                public void run() {
                    timedout.set(true);
                }};
            timer.schedule(task, BUFFER_TIMEOUT);
            uninterruptably(() -> {
                ByteBuffer next;
                // while we still haven't received a new buffer we keep checking
                while ((next = buffers.poll(1, SECONDS)) == null) 
                    {if (closed || finished) return; 
                    if (timedout.get()) {
                        log.warn("Timed out waiting {}ms for a buffer!", BUFFER_TIMEOUT);
                        return;
                    }}
                    
                current = next;
            });
        }

        private void checkCurrent() {
            if (current == INITIAL) next();
            if (finished && !current.hasRemaining()) close();
        }

        @Override
        public long skip(long n) {
            checkCurrent();
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
                // there are fresh buffers so recurse
                return skip + skip(n - skip);
            }
            return skip;
        }

        @Override
        public int read(byte[] b, int offset, int length) {
            checkCurrent();
            if (closed) return DONE;
            if (length == 0) return 0;
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
            // how many bytes have we gotten?
            int transferred = available;
            // if there is another buffer, read from it too
            ByteBuffer last = current;
            next();
            // if no more fresh buffers, we have done as much as we can
            if (last == current) return transferred;
            // otherwise there are fresh buffers, so recurse
            int nextRead = read(b, offset + transferred, toGo);
            // if we successfully read from fresh buffer(s) record how many bytes
            if (nextRead != -1) transferred += nextRead;
            return transferred == 0 ? DONE : transferred;
        }

        /**
         * Call to indicate that no further buffers will be offered.
         */
        void finish() {
            finished = true;
        }

        @Override
        public void close() {
            closed = true;
            finishWith.cancel(true);
            buffers.clear();
        }

        @Override
        public int read() {
            checkCurrent();
            if (closed) return DONE;
            if (current.hasRemaining()) return Byte.toUnsignedInt(current.get());
            if (finished) {
                // no more bytes
                close();
                return DONE;
            }
            ByteBuffer last = current;
            next();
            // if no fresh buffers we are done
            if (last == current) return DONE;
            // otherwise there are fresh buffers, so recurse
            return read();
        }
    }
}
