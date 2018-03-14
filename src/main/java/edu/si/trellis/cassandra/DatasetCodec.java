package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.atlas.io.IO.closeSilent;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.RDFDataMgr.writeQuads;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RiotException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

public class DatasetCodec extends TypeCodec<Dataset> {
    
    public static final DatasetCodec datasetCodec = new DatasetCodec();

    private static final JenaRDF rdf = new JenaRDF();
    
    public DatasetCodec() {
        super(DataType.text(), Dataset.class);
    }

    @Override
    public ByteBuffer serialize(Dataset dataset, ProtocolVersion protocolVersion) {
        if (dataset == null || dataset.size() == 0) return null;
        return ByteBuffer.wrap(toNQuads(dataset));
    }

    private byte[] toNQuads(Dataset dataset) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            writeQuads(bytes, dataset.stream().map(rdf::asJenaQuad).iterator());
            return bytes.toByteArray();
        } catch (RiotException e) {
            throw new InvalidTypeException("Dataset is impossible to serialize!", e);
        } finally {
            closeSilent(bytes);
        }
    }

    @Override
    public Dataset deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? rdf.createDataset() : fromNQuads(Bytes.getArray(bytes));
    }

    private Dataset fromNQuads(byte[] bytes) {
        org.apache.jena.query.Dataset dataset = DatasetFactory.create();
        try {
            read(dataset, new ByteArrayInputStream(bytes), null, NQUADS);
            return rdf.asDataset(dataset);
        } catch (RiotException e) {
            throw new InvalidTypeException("Dataset is impossible to deserialize!", e);
        }
    }

    @Override
    public Dataset parse(String graph) {
        if (graph == null || graph.isEmpty()) return rdf.createDataset();
        return fromNQuads(graph.getBytes());
    }

    @Override
    public String format(Dataset dataset) {
        if (dataset == null || dataset.size() == 0) return null;
        return new String(toNQuads(dataset), UTF_8);
    }

}
