package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;

import java.time.Instant;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;

class ResourceQueryContext extends QueryContext {

    private static final Logger log = getLogger(ResourceQueryContext.class);

    private static final String GET_QUERY = "SELECT * FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? AND "
                    + "createdSeconds <= ? LIMIT 1 ALLOW FILTERING;";

    private static final String DELETE_QUERY = "DELETE FROM " + MUTABLE_TABLENAME + " WHERE identifier = ? ";

    private static final String IMMUTABLE_INSERT_QUERY = "INSERT INTO " + IMMUTABLE_TABLENAME
                    + " (identifier, quads, created) VALUES (?,?,?)";

    private static final String MUTABLE_INSERT_QUERY = "INSERT INTO " + MUTABLE_TABLENAME
                    + " (interactionModel, mimeType, createdSeconds, container, quads, modified, binaryIdentifier, created, identifier)"
                    + " VALUES (?,?,?,?,?,?,?,?,?)";

    private static final String TOUCH_QUERY = "UPDATE " + MUTABLE_TABLENAME
                    + " SET modified=? WHERE created=? AND identifier=?";

    private static final String MEMENTOS_QUERY = "SELECT modified FROM " + MUTABLE_TABLENAME + " WHERE identifier = ?";

    private static final String mutableQuadStreamQuery = "SELECT quads FROM " + MUTABLE_TABLENAME
                    + "  WHERE identifier = ? AND createdSeconds <= ? LIMIT 1 ALLOW FILTERING;";

    private static final String immutableQuadStreamQuery = "SELECT quads FROM " + IMMUTABLE_TABLENAME
                    + "  WHERE identifier = ? ;";

    private static final String basicContainmentQuery = "SELECT identifier AS contained FROM "
                    + BASIC_CONTAINMENT_TABLENAME + " WHERE container = ? ;";

    private final PreparedStatement getStatement, immutableInsertStatement, deleteStatement, mutableInsertStatement,
                    touchStatement, mementosStatement, mutableQuadStreamStatement, immutableQuadStreamStatement,
                    basicContainmentStatement;

    @Inject
    ResourceQueryContext(Session session, @RdfReadConsistency ConsistencyLevel readConsistency,
                    @RdfWriteConsistency ConsistencyLevel writeConsistency) {
        super(session);
        log.debug("Preparing retrieval query: {}", GET_QUERY);
        this.getStatement = session.prepare(GET_QUERY).setConsistencyLevel(readConsistency);
        log.debug("Preparing deletion query: {}", DELETE_QUERY);
        this.deleteStatement = session.prepare(DELETE_QUERY).setConsistencyLevel(writeConsistency);
        log.debug("Preparing immmutable data insert query: {}", IMMUTABLE_INSERT_QUERY);
        this.immutableInsertStatement = session.prepare(IMMUTABLE_INSERT_QUERY).setConsistencyLevel(writeConsistency);
        log.debug("Preparing mutable data insert statement: {}", MUTABLE_INSERT_QUERY);
        this.mutableInsertStatement = session.prepare(MUTABLE_INSERT_QUERY).setConsistencyLevel(writeConsistency);
        log.debug("Preparing touch data update statement: {}", TOUCH_QUERY);
        this.touchStatement = session.prepare(TOUCH_QUERY).setConsistencyLevel(writeConsistency);
        log.debug("Preparing Mementos data retrieval statement: {}", MEMENTOS_QUERY);
        this.mementosStatement = session.prepare(MEMENTOS_QUERY).setConsistencyLevel(readConsistency);

        this.mutableQuadStreamStatement = session.prepare(mutableQuadStreamQuery).setConsistencyLevel(readConsistency);
        this.immutableQuadStreamStatement = session.prepare(immutableQuadStreamQuery)
                        .setConsistencyLevel(readConsistency);
        this.basicContainmentStatement = session.prepare(basicContainmentQuery).setConsistencyLevel(readConsistency);
    }

    CompletableFuture<ResultSet> get(IRI id, Instant time) {
        return executeRead(getStatement.bind(id, time));
    }

    CompletableFuture<Void> touch(Instant modified, UUID created, IRI id) {
        return executeWrite(touchStatement.bind(modified, created, id));
    }

    CompletableFuture<ResultSet> mementos(IRI id) {
        return executeRead(mementosStatement.bind(id));
    }

    CompletableFuture<Void> mutate(IRI ixnModel, String mimeType, Instant createdSeconds, IRI container,
                    Dataset data, Instant modified, IRI binaryIdentifier, UUID creation, IRI id) {
        return executeWrite(mutableInsertStatement.bind(ixnModel, mimeType, createdSeconds, container, data,
                        modified, binaryIdentifier, creation, id));
    }

    CompletableFuture<Void> delete(IRI id) {
        return executeWrite(deleteStatement.bind(id));
    }

    ResultSet containment(IRI id) {
        return executeSyncRead(basicContainmentStatement.bind(id));
    }

    Stream<Quad> mutableQuadStream(IRI id, Long time) {
        return quadStreamFromQuery(mutableQuadStreamStatement.bind(id, time));
    }

    Stream<Quad> immutableQuadStream(IRI id) {
        return quadStreamFromQuery(immutableQuadStreamStatement.bind(id));
    }

    CompletableFuture<Void> immutate(IRI id, Dataset data, Instant time) {
        return executeWrite(immutableInsertStatement.bind(id, data, time));
    }

    protected Stream<Quad> quadStreamFromQuery(final Statement boundStatement) {
        final Spliterator<Row> rows = executeSyncRead(boundStatement).spliterator();
        Stream<Dataset> datasets = StreamSupport.stream(rows, false).map(r -> r.get("quads", Dataset.class));
        return datasets.flatMap(Dataset::stream);
    }
}