package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.query.DatasetFactory.createGeneral;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.writer.NQuadsWriter.write;

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

public class DatasetCodec extends TypeCodec<Dataset> {

    private static final int AVERAGE_QUAD_SIZE = 100;

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private static final JenaRDF rdf = new JenaRDF();

    protected DatasetCodec(DataType cqlType) {
        super(cqlType, Dataset.class);
    }

    @Override
    public ByteBuffer serialize(Dataset dataset, ProtocolVersion protocolVersion) {
        if (dataset == null || dataset.size() == 0) return EMPTY_BYTE_BUFFER;
        return ByteBuffer.wrap(toNQuads(dataset));
    }

    private byte[] toNQuads(Dataset dataset) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream((int) (dataset.size() * AVERAGE_QUAD_SIZE));
        write(buffer, dataset.stream().map(rdf::asJenaQuad).iterator());
        return buffer.toByteArray();
    }

    @Override
    public Dataset deserialize(ByteBuffer buf, ProtocolVersion protocolVersion) {
        final byte[] bytes;
        if (buf.hasArray()) bytes = buf.array();
        else {
            bytes = new byte[buf.remaining()];
            // avoid jostling the input buffer's position by duplicating it
            buf.duplicate().get(bytes);
        }
        if (bytes == null || bytes.length == 0) return rdf.createDataset();
        org.apache.jena.query.Dataset dataset = DatasetFactory.createGeneral();
        read(dataset, new ByteArrayInputStream(bytes), NQUADS);
        return rdf.asDataset(dataset);
    }

    @Override
    public Dataset parse(String quads) {
        if (quads == null || quads.isEmpty() || quads.equals("NULL")) return null;
        org.apache.jena.query.Dataset dataset = createGeneral();
        try {
            read(dataset, new ByteArrayInputStream(quads.getBytes(UTF_8)), NQUADS);
        } catch (RiotException e) {
            throw new InvalidTypeException("Ill-formed NQuads!", e);
        }
        return rdf.asDataset(dataset);
    }

    @Override
    public String format(Dataset dataset) {
        return dataset == null ? "NULL" : new String(toNQuads(dataset), UTF_8);
    }
}
