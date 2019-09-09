package edu.si.trellis;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.api.TrellisUtils;

/**
 * (De)serializes Commons RDF {@link IRI}s (out of)into Cassandra fields.
 * 
 * @author ajs6f
 *
 */
class IRICodec extends CassandraCodec implements TypeCodec<IRI> {

    private static final GenericType<IRI> IRI_TYPE = GenericType.of(IRI.class);

    /**
     * Singleton instance.
     */
    static final IRICodec iriCodec = new IRICodec();

    protected static final int CACHE_MAXIMUM_SIZE = 10 ^ 6;

    protected static final RDF rdf = TrellisUtils.getInstance();

    @Override
    public DataType getCqlType() {
        return TEXT;
    }

    @Override
    public GenericType<IRI> getJavaType() {
        return IRI_TYPE;
    }

    @Override
    public String format(IRI v) {
        return v != null ? v.getIRIString() : null;
    }

    @Override
    public ByteBuffer encode(IRI iri, ProtocolVersion protocolVersion) {
        return iri != null ? wrap(format(iri).getBytes(UTF_8)) : null;
    }

    @Override
    public IRI decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? null : parse(new String(bytesFromBuffer(bytes), UTF_8));
    }

    @Override
    public IRI parse(String v) {
        if (v == null || v.isEmpty()) return null;
        return rdf.createIRI(v);
    }
}
