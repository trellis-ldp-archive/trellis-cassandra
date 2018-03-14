package edu.si.trellis.cassandra;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

public class QuadCodec extends TypeCodec<Quad> {

    public QuadCodec() {
        super(DataType.text(), Quad.class);
    }

    @Override
    public ByteBuffer serialize(Quad q, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return q != null ? wrap(format(q).getBytes(UTF_8)) : null;
    }

    @Override
    public Quad deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return bytes == null || bytes.remaining() == 0 ? null : parse(new String(Bytes.getArray(bytes), UTF_8));
    }

    @Override
    public Quad parse(String value) throws InvalidTypeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String format(Quad q) throws InvalidTypeException {
        String subject = q.getSubject().ntriplesString();
        String predicate = q.getPredicate().ntriplesString();
        String object = q.getObject().ntriplesString();
        String graphName = q.getGraphName().map(RDFTerm::ntriplesString).orElse("");
        return String.format("%1$s %2$s %2$s %4$s .", subject, predicate, object, graphName);
    }

}
