package edu.si.trellis.cassandra;

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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class IRICodec extends TypeCodec<IRI> {
    
    public static final IRICodec iriCodec = new IRICodec();

    private static final int concurrencyLevel = 16;

    Cache<String, IRI> cache = CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel).build();

    private static final JenaRDF rdf = new JenaRDF();

    protected IRICodec(DataType cqlType) {
        super(cqlType, IRI.class);
    }

    public IRICodec() {
        this(DataType.text());
    }

    @Override
    public ByteBuffer serialize(IRI iri, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return wrap(format(iri).getBytes(UTF_8));
    }

    @Override
    public IRI deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return parse(new String(Bytes.getArray(bytes), UTF_8));
    }

    @Override
    public IRI parse(String iri) throws InvalidTypeException {
        try {
            return cache.get(iri, () -> rdf.createIRI(iri));
        } catch (Exception e) {
            throw new InvalidTypeException("Bad URI!", e);
        }
    }

    @Override
    public String format(IRI iri) throws InvalidTypeException {
        return iri.getIRIString();
    }

}
