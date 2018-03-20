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

/**
 * (De)serializes Commons RDF {@link IRI}s (out of)into Cassandra fields.
 * 
 * @author ajs6f
 *
 */
public class IRICodec extends TypeCodec<IRI> {

    /**
     * Singleton instance.
     */
    public static final IRICodec iriCodec = new IRICodec();

    protected static final int cacheConcurrencyLevel = 16;

    protected static final long cacheMaximumSize = 10 ^ 6;

    protected static final JenaRDF rdf = new JenaRDF();

    private final LoadingCache<String, IRI> cache = newBuilder().concurrencyLevel(cacheConcurrencyLevel)
                    .maximumSize(cacheMaximumSize).build(from(this::deserialize));

    /**
     * Default constructor.
     */
    public IRICodec() {
        super(DataType.text(), IRI.class);
    }

    private IRI deserialize(String v) {
        return rdf.createIRI(v);
    }

    @Override
    public String format(IRI v) {
        return v != null ? v.getIRIString() : null;
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
    public IRI parse(String v) throws InvalidTypeException {
        if (v == null || v.isEmpty()) return null;
        try {
            return cache.get(v);
        } catch (Exception e) {
            throw new InvalidTypeException("Bad URI!", e);
        }
    }
}
