package edu.si.trellis;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import edu.si.trellis.NoopCloseCountingInputStream;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("resource")
class CountingInputStreamTest {

    @Mock
    private InputStream mockInputStream;

    @Test
    void doNotPassClose() throws IOException {
        new NoopCloseCountingInputStream(mockInputStream).close();
        verify(mockInputStream, never()).close();
    }
}
