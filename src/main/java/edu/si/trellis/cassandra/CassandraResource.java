package edu.si.trellis.cassandra;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.trellisldp.api.RDFUtils;
import org.trellisldp.api.Resource;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import static edu.si.trellis.cassandra.CassandraResourceService.Mutability.*;

public class CassandraResource implements Resource {

    private static final RDF RDF = RDFUtils.getInstance();

    /**
     * Same-thread execution. TODO optimize with a threadpool
     */
    private final Executor executor = Runnable::run;

    public static final String mutableQuadStreamQuery = "SELECT quads FROM " + Mutable.tableName
                    + "  WHERE identifier = ? ;";

    public static final String immutableQuadStreamQuery = "SELECT quads FROM " + Immutable.tableName
                    + "  WHERE identifier = ? ;";

    public static final String metadataQuery = "SELECT * FROM " + Meta.tableName + " WHERE identifier = ? LIMIT 1 ;";

    private final PreparedStatement immutableQuadStreamStatement, mutableQuadStreamStatement, metadataStatement;

    private Session session;

    private final IRI identifier;

    public IRI parent;

    private volatile IRI interactionModel;

    private volatile Boolean hasAcl;

    private volatile Instant modified;

    public CassandraResource(final IRI identifier, final IRI ixnModel, final Session session) {
        this.identifier = requireNonNull(identifier);
        this.session = requireNonNull(session);
        this.interactionModel = ixnModel;
        this.mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery);
        this.immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery);
        this.metadataStatement = session.prepare(metadataQuery);
    }

    public CassandraResource(final IRI identifier, final Session session) {
        this(identifier, null, session);
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

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
                    modified = metadata.get("modified", Instant.class);
                    interactionModel = metadata.get("interactionModel", IRI.class);
                    parent = metadata.get("parent", IRI.class);
                }
            }
        }
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
