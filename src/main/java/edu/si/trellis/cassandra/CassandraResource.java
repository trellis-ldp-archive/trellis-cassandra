package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Meta;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.api.Binary;
import org.trellisldp.api.RDFUtils;
import org.trellisldp.api.Resource;
import org.trellisldp.vocabulary.LDP;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraResource implements Resource {

    private static final RDF RDF = RDFUtils.getInstance();

    public static final String mutableQuadStreamQuery = "SELECT quads FROM " + Mutable.tableName
                    + "  WHERE identifier = ? ;";

    public static final String immutableQuadStreamQuery = "SELECT quads FROM " + Immutable.tableName
                    + "  WHERE identifier = ? ;";

    public static final String metadataQuery = "SELECT identifier, interactionModel, hasAcl, binaryIdentifier, "
                    + "mimeType, size, parent, WRITETIME(interactionModel) AS modified FROM " + Meta.tableName
                    + " WHERE identifier = ? LIMIT 1 ;";

    private static PreparedStatement immutableQuadStreamStatement, mutableQuadStreamStatement, metadataStatement;

    private Session session;

    private final IRI identifier;

    private volatile IRI binaryIdentifier, parent, interactionModel;
    
    private volatile String mimeType;

    private volatile long size;
    
    private volatile Boolean hasAcl;

    private volatile Instant modified;

    public CassandraResource(final IRI identifier, final Session session) {
        this.identifier = requireNonNull(identifier);
        this.session = requireNonNull(session);
        // don't need to synchronize because preparing twice is inefficient but not a fault
        if (mutableQuadStreamStatement == null) prepareQueries();
    }

    private void prepareQueries() {
        mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery);
        immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery);
        metadataStatement = session.prepare(metadataQuery);
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    /**
     * @return a container for this resource
     */
    public IRI getParent() {
        computeMetadata();
        return parent;
    }

    @Override
    public IRI getInteractionModel() {
        computeMetadata();
        return interactionModel;
    }

    @Override
    public Instant getModified() {
        computeMetadata();
        return modified;
    }

    @Override
    public Boolean hasAcl() {
        computeMetadata();
        return hasAcl;
    }

    private void computeMetadata() {
        // use any memoized state for this because it all gets set together below
        Boolean result = hasAcl;
        if (result == null) { // First check (no locking)
            synchronized (this) {
                result = hasAcl;
                if (result == null) { // Second check (with locking)
                    final Row metadata = fetchMetadata();
                    hasAcl = metadata.getBool("hasAcl");
                    modified = Instant.ofEpochMilli(metadata.get("modified", Long.class));
                    interactionModel = metadata.get("interactionModel", IRI.class);
                    binaryIdentifier = metadata.get("binaryIdentifier", IRI.class);
                    mimeType = metadata.getString("mimetype");
                    size = metadata.getLong("size");
                    parent = metadata.get("parent", IRI.class);
                }
            }
        }
    }
    
    @Override
    public Optional<Binary> getBinary() {
        return Optional.ofNullable(isBinary() ? new Binary(binaryIdentifier, modified, mimeType, size) : null);
    }

    private boolean isBinary() {
        return LDP.NonRDFSource.equals(getInteractionModel());
    }

    private Row fetchMetadata() {
        final BoundStatement boundStatement = metadataStatement.bind(identifier);
        return session.execute(boundStatement).one();
    }

    @Override
    public Stream<? extends Quad> stream() {
        BoundStatement boundStatement = mutableQuadStreamStatement.bind(identifier);
        Stream<Quad> mutableQuads = quadStreamQuery(boundStatement);
        boundStatement = immutableQuadStreamStatement.bind(identifier);
        Stream<Quad> immutableQuads = quadStreamQuery(boundStatement);
        return Stream.concat(mutableQuads, immutableQuads);
    }

    private Stream<Quad> quadStreamQuery(final BoundStatement boundStatement) {
        final Spliterator<Row> rows = session.execute(boundStatement).spliterator();
        return StreamSupport.stream(rows, false).flatMap(row -> row.get("quads", Dataset.class).stream());
    }
}
