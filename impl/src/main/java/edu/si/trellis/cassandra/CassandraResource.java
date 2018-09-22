package edu.si.trellis.cassandra;

import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Immutable;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.Mutable;
import static java.util.Objects.requireNonNull;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.Binary;
import org.trellisldp.api.Resource;
import org.trellisldp.vocabulary.LDP;

class CassandraResource implements Resource {

    public static final String mutableQuadStreamQuery = "SELECT quads FROM trellis." + Mutable.tableName
                    + "  WHERE identifier = ? LIMIT 1 ;";

    public static final String immutableQuadStreamQuery = "SELECT quads FROM trellis." + Immutable.tableName
                    + "  WHERE identifier = ? ;";

    public static final String metadataQuery = "SELECT identifier, interactionModel, hasAcl, binaryIdentifier, "
                    + "mimeType, size, parent, WRITETIME(interactionModel) AS modified FROM trellis." + Mutable.tableName
                    + " WHERE identifier = ? LIMIT 1 ;";

    private PreparedStatement immutableQuadStreamStatement, mutableQuadStreamStatement, metadataStatement;

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
        prepareQueries();
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
    public Optional<IRI> getParent() {
        computeMetadata();
        return Optional.ofNullable(parent);
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
        // use any piece of memoized state for this test because it all gets set together below
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
        Stream<Quad> mutableQuads = quadStreamFromQuery(boundStatement);
        boundStatement = immutableQuadStreamStatement.bind(identifier);
        Stream<Quad> immutableQuads = quadStreamFromQuery(boundStatement);
        return Stream.concat(mutableQuads, immutableQuads);
    }

    private Stream<Quad> quadStreamFromQuery(final BoundStatement boundStatement) {
        final Spliterator<Row> rows = session.execute(boundStatement).spliterator();
        return StreamSupport.stream(rows, false).flatMap(row -> row.get("quads", Dataset.class).stream());
    }
}
