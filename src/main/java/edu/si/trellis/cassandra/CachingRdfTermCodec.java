package edu.si.trellis.cassandra;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.cache.CacheLoader.from;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.jena.JenaRDF;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.cache.LoadingCache;

public abstract class CachingRdfTermCodec<T extends RDFTerm> extends TypeCodec<T> {

    private static final int cacheConcurrencyLevel = 16;

    private static final long cacheMaximumSize = 10 ^ 6;

    private static final JenaRDF rdf = new JenaRDF();

    @SuppressWarnings("unchecked")
    private T deserialize(String v) {
        switch (v.charAt(0)) {
        case '_':
            return (T) rdf.createBlankNode(v.substring(2));
        case '"':
            rdf.createLiteral(unwrap(v));
        default:
            return (T) rdf.createIRI(unwrap(v));
        }
    };

    /**
     * Remove wrappers <> from IRIs and "" from literals in NTriples
     * 
     * @param v
     * @return v sans wrappers
     */
    private String unwrap(String v) {
        return v.substring(1, v.length() - 1);
    }

    private final LoadingCache<String, T> cache = newBuilder().concurrencyLevel(cacheConcurrencyLevel)
                    .maximumSize(cacheMaximumSize).build(from(this::deserialize));

    protected CachingRdfTermCodec(Class<T> javaClass) {
        super(DataType.text(), javaClass);
    }

    @Override
    public String format(T v) {
        return v != null ? v.ntriplesString() : null;
    }

    @Override
    public ByteBuffer serialize(T iri, ProtocolVersion protocolVersion) {
        return iri != null ? wrap(format(iri).getBytes(UTF_8)) : null;
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return bytes == null || bytes.remaining() == 0 ? null : parse(new String(Bytes.getArray(bytes), UTF_8));
    }

    @Override
    public T parse(String v) throws InvalidTypeException {
        if (v == null || v.isEmpty()) return null;
        try {
            return cache.get(v);
        } catch (Exception e) {
            throw new InvalidTypeException("Bad URI!", e);
        }
    }

}
