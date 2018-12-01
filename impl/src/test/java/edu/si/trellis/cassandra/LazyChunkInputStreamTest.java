package edu.si.trellis.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.trellisldp.api.RuntimeTrellisException;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("resource")
public class LazyChunkInputStreamTest {

    @Mock
    private Session mockSession;

    @Mock
    private BoundStatement mockQuery;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private Row mockRow;

    @Mock
    private InputStream mockInputStream;

    private byte[] b = null;
    private int off = 0, len = 0, n = 0, readlimit = 0;

    @Test
    public void badQuery() {
        RuntimeException e = new RuntimeException("Expected");
        when(mockSession.execute(mockQuery)).thenThrow(e);

        LazyChunkInputStream testLazyChunkInputStream = new LazyChunkInputStream(mockSession, mockQuery);

        try {
            testLazyChunkInputStream.read();
        } catch (Exception e1) {
            assertSame(e, e1, "Didn't get the exception we expected!");
        }
    }

    @Test
    public void noData() {
        when(mockSession.execute(mockQuery)).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(null);

        LazyChunkInputStream testLazyChunkInputStream = new LazyChunkInputStream(mockSession, mockQuery);

        try {
            testLazyChunkInputStream.read();
        } catch (Exception e) {
            assertThat("Wrong exception type!", e, instanceOf(RuntimeTrellisException.class));
            assertEquals("Missing binary chunk!", e.getMessage(), "Wrong exception message!");
        }
    }

    @Test
    public void normalOperation() throws IOException {
        when(mockSession.execute(mockQuery)).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.get("chunk", InputStream.class)).thenReturn(mockInputStream);

        LazyChunkInputStream testLazyChunkInputStream = new LazyChunkInputStream(mockSession, mockQuery);

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

        testLazyChunkInputStream.close();
        verify(mockInputStream).close();
    }
}
