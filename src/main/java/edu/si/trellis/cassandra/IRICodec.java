package edu.si.trellis.cassandra;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.cache.CacheLoader.from;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.jena.JenaRDF;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.cache.LoadingCache;

public class IRICodec extends TypeCodec<IRI> {

    public static final IRICodec iriCodec = new IRICodec();

    private static final int iriCacheConcurrencyLevel = 16;

    private static final long iriCacheMaximumSize = 10 ^ 6;

    private static final JenaRDF rdf = new JenaRDF();

    private static final LoadingCache<String, IRI> iriCache = newBuilder().concurrencyLevel(iriCacheConcurrencyLevel)
                    .maximumSize(iriCacheMaximumSize).build(from(rdf::createIRI));

    protected IRICodec(DataType cqlType) {
        super(cqlType, IRI.class);
    }

    public IRICodec() {
        this(DataType.text());
    }

    @Override
    public ByteBuffer serialize(IRI iri, ProtocolVersion protocolVersion) {
        return iri != null ? wrap(format(iri).getBytes(UTF_8)) : null;
    }

    @Override
    public IRI deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return bytes == null || bytes.remaining() == 0 ? null : parse(new String(Bytes.getArray(bytes), UTF_8));
    }

    @Override
    public IRI parse(String iri) throws InvalidTypeException {
        if (iri == null || iri.isEmpty()) return null;
        try {
            return iriCache.get(iri);
        } catch (Exception e) {
            throw new InvalidTypeException("Bad URI!", e);
        }
    }

    @Override
    public String format(IRI iri) {
        return iri != null ? iri.getIRIString() : null;
    }
}
