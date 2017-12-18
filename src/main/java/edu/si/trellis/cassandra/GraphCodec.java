package edu.si.trellis.cassandra;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

public class GraphCodec extends TypeCodec<Graph> {

    private static final JenaRDF rdf = new JenaRDF();

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    protected GraphCodec(DataType cqlType) {
        super(cqlType, Graph.class);
    }

    @Override
    public ByteBuffer serialize(Graph graph, ProtocolVersion protocolVersion) {
        if (graph == null || graph.size() == 0) return EMPTY_BYTE_BUFFER;
        return ByteBuffer.wrap(toNtriples(graph));
    }

    private byte[] toNtriples(Graph graph) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            RDFDataMgr.writeTriples(bytes, rdf.asJenaGraph(graph).find());
            return bytes.toByteArray();
        } catch (RiotException e) {
            throw new InvalidTypeException("Graph is impossible to serialize!", e);
        } finally {
            IO.closeSilent(bytes);
        }
    }

    @Override
    public Graph deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? rdf.createGraph() : fromNtriples(Bytes.getArray(bytes));
    }

    private Graph fromNtriples(byte[] bytes) {
        Model model = ModelFactory.createDefaultModel();
        try {
            return rdf.asGraph(model.read(new ByteArrayInputStream(bytes), "", "N-TRIPLE"));
        } catch (RiotException e) {
            throw new InvalidTypeException("Graph is impossible to deserialize!", e);
        }
    }

    @Override
    public Graph parse(String graph) {
        if (graph == null || graph.isEmpty()) return rdf.createGraph();
        return fromNtriples(graph.getBytes());
    }

    @Override
    public String format(Graph graph) {
        if (graph == null || graph.size() == 0) return "";
        return new String(toNtriples(graph), UTF_8);
    }

}
