package edu.si.trellis;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LazyChunkInputStreamTest {

    @Mock
    private CqlSession mockSession;

    @Mock
    private BoundStatement mockQuery;

    @Mock
    private AsyncResultSet mockResultSet;

    @Mock
    private Row mockRow;

    @Mock
    private InputStream mockInputStream;

    private byte[] b = null;

    private int off = 0, len = 0, n = 0, readlimit = 0;

    @Test
    void badQuery() {
        RuntimeException e = new RuntimeException("Expected");
        when(mockSession.executeAsync(mockQuery)).thenThrow(e);
        try (LazyChunkInputStream testLazyChunkInputStream = new LazyChunkInputStream(mockSession, mockQuery)) {
            testLazyChunkInputStream.read();
        } catch (Exception e1) {
            assertSame(e, e1, "Didn't get the exception we expected!");
        }
    }

//    @Test
//    void noData() {
//        when(mockSession.executeAsync(mockQuery)).thenReturn(completedFuture(mockResultSet));
//        when(mockResultSet.one()).thenReturn(null);
//
//        try (LazyChunkInputStream testLazyChunkInputStream = new LazyChunkInputStream(mockSession, mockQuery)) {
//            testLazyChunkInputStream.read();
//        } catch (Exception e) {
//            Throwable cause = e.getCause();
//            assertThat("Wrong exception type!", cause, instanceOf(NullPointerException.class));
//            assertEquals("Missing binary chunk!", cause.getMessage(), "Wrong exception message!");
//        }
//    }

    @Test
    void normalOperation() throws IOException {
        when(mockSession.executeAsync(mockQuery)).thenReturn(completedFuture(mockResultSet));
        when(mockSession.executeAsync(mockQuery)).thenReturn(completedFuture(mockResultSet));
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.get("chunk", InputStream.class)).thenReturn(mockInputStream);

        try (LazyChunkInputStream testLazyChunkInputStream = new LazyChunkInputStream(mockSession, mockQuery)) {

            testLazyChunkInputStream.read();
            verify(mockInputStream).read();

            testLazyChunkInputStream.read(b);
            verify(mockInputStream).read(b);

            testLazyChunkInputStream.read(b, off, len);
            verify(mockInputStream).read(b, off, len);

            testLazyChunkInputStream.skip(n);
            verify(mockInputStream).skip(n);

            testLazyChunkInputStream.available();
            verify(mockInputStream).available();

            testLazyChunkInputStream.mark(readlimit);
            verify(mockInputStream).mark(readlimit);

            testLazyChunkInputStream.reset();
            verify(mockInputStream).reset();

            testLazyChunkInputStream.markSupported();
            verify(mockInputStream).markSupported();

        }
        verify(mockInputStream).close();
    }
}
