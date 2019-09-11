package edu.si.trellis.query;

import static java.util.Spliterators.spliteratorUnknownSize;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A simple {@link Spliterator} backed by a {@link AsyncResultSet}.
 *
 * Not thread-safe!
 */
public class AsyncResultSetSpliterator implements Spliterator<Row> {

    private final AsyncResultSet results;

    private Iterator<Row> currentResults;

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.NONNULL;

    /**
     * @param results an {@link AsyncResultSet}
     * @return a {@link Stream} of {@link Rows}s
     */
    public static Stream<Row> stream(AsyncResultSet results) {
        return StreamSupport.stream(new AsyncResultSetSpliterator(results), false);
    }

    public AsyncResultSetSpliterator(AsyncResultSet r) {
        this.results = r;
        this.currentResults = r.currentPage().iterator();
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        if (currentResults.hasNext()) {
            action.accept(currentResults.next());
            return true;
        }
        if (results.hasMorePages()) {
            nextPage();
            return tryAdvance(action);
        }
        return false;
    }

    @Override
    public Spliterator<Row> trySplit() {
        if (results.hasMorePages()) {
            final Iterator<Row> splitResults = currentResults;
            nextPage();
            return spliteratorUnknownSize(splitResults, 0);
        }
        return null;
    }

    private void nextPage() {
        if (currentResults.hasNext()) return;
        results.fetchNextPage();
        currentResults = results.currentPage().iterator();
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return CHARACTERISTICS;
    }
}
