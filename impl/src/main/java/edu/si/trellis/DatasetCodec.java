package edu.si.trellis;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.RDFDataMgr.writeQuads;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.query.DatasetFactory;

class DatasetCodec extends CassandraCodec implements TypeCodec<Dataset> {

    private static final GenericType<Dataset> DATASET_TYPE = GenericType.of(Dataset.class);

    static final DatasetCodec datasetCodec = new DatasetCodec();

    private static final JenaRDF rdf = new JenaRDF();

    @Override
    public DataType getCqlType() {
        return TEXT;
    }

    @Override
    public GenericType<Dataset> getJavaType() {
        return DATASET_TYPE;
    }

    @Override
    public ByteBuffer encode(Dataset dataset, ProtocolVersion protocolVersion) {
        if (dataset == null || dataset.size() == 0) return null;
        return ByteBuffer.wrap(toNQuads(dataset));
    }

    private byte[] toNQuads(Dataset dataset) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            writeQuads(bytes, dataset.stream().map(rdf::asJenaQuad).iterator());
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Dataset could not be serialized!", e);
        }
    }

    @Override
    public Dataset decode(ByteBuffer buffer, ProtocolVersion protocolVersion) {
        if (buffer == null) return rdf.createDataset();
        return fromNQuads(bytesFromBuffer(buffer));
    }

    private Dataset fromNQuads(byte[] bytes) {
        org.apache.jena.query.Dataset dataset = DatasetFactory.create();
        read(dataset, new ByteArrayInputStream(bytes), null, NQUADS);
        return rdf.asDataset(dataset);
    }

    @Override
    public Dataset parse(String quads) {
        if (quads == null || quads.isEmpty()) return rdf.createDataset();
        return fromNQuads(quads.getBytes(UTF_8));
    }

    @Override
    public String format(Dataset dataset) {
        if (dataset == null || dataset.size() == 0) return null;
        return new String(toNQuads(dataset), UTF_8);
    }
}
