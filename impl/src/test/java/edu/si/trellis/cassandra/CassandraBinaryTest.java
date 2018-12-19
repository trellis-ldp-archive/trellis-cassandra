package edu.si.trellis.cassandra;

import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.*;

import java.io.InputStream;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.trellisldp.api.RuntimeTrellisException;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("resource")
public class CassandraBinaryTest {

    private final RDF factory = new SimpleRDF();

    private int testChunkSize;

    private final IRI testId = factory.createIRI("urn:test");

    @Mock
    private PreparedStatement mockPreparedStatement1, mockPreparedStatement2;

    @Mock
    private Session mockSession, mockSession2;

    @Mock
    private BoundStatement mockBoundStatement1, mockBoundStatement2;

    @Mock
    private ResultSet mockResultSet1, mockResultSet2;

    private Spliterator<Row> testSpliterator;

    @Mock
    private Row mockRow;

    @Mock
    private InputStream mockInputStream;

    @Test
    public void noContent() {
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement1);
        when(mockPreparedStatement1.bind(testId)).thenReturn(mockBoundStatement1);
        when(mockPreparedStatement1.setConsistencyLevel(any())).thenReturn(mockPreparedStatement1);
        BinaryQueryContext testContext = new BinaryQueryContext(mockSession, ONE, ONE);
        when(mockSession.execute(mockBoundStatement1)).thenReturn(mockResultSet1);
        testSpliterator = new TestRowSpliterator(0, mockRow);
        when(mockResultSet1.spliterator()).thenReturn(testSpliterator);

        CassandraBinary testCassandraBinary = new CassandraBinary(testId, testContext, testChunkSize);

        try {
            testCassandraBinary.getContent();
        } catch (Exception e) {
            assertThat("Wrong exception type!", e, instanceOf(RuntimeTrellisException.class));
            assertEquals("Binary not found under IRI: urn:test", e.getMessage(), "Wrong exception message!");
        }
    }

    private static class TestRowSpliterator implements Spliterator<Row> {

        private long size;

        private Row row;

        public TestRowSpliterator(long size, Row row) {
            this.size = size;
            this.row = row;
        }

        @Override
        public boolean tryAdvance(Consumer<? super Row> action) {
            boolean nonEmpty = size-- != 0;
            if (nonEmpty) action.accept(row);
            return nonEmpty;
        }

        @Override
        public Spliterator<Row> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return size;
        }

        @Override
        public int characteristics() {
            return 0;
        }
    }
}
