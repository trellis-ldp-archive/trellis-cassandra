package edu.si.trellis;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

import java.nio.ByteBuffer;
import java.util.Arrays;

abstract class CassandraCodec<T>  implements TypeCodec<T> {

    protected byte[] bytesFromBuffer(ByteBuffer buffer) {
        int length = buffer.remaining();

        // can we go get the buffer's backing array?
        if (buffer.hasArray()) {
            int offset = buffer.arrayOffset() + buffer.position();
            final byte[] bufferArray = buffer.array();
            // try and take the array wholesale
            if (offset == 0 && length == bufferArray.length) return bufferArray;
            // but if we can't, copy out the relevant range
            return Arrays.copyOfRange(bufferArray, offset, offset + length);
        }
        // no backing array, so we have to copy the buffer and copy the bytes out
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);
        return bytes;
    }

}
