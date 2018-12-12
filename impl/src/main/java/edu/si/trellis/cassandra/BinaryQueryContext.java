package edu.si.trellis.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import javax.inject.Inject;

class BinaryQueryContext extends QueryContext {

    private static final String INSERT_QUERY = "INSERT INTO Binarydata (identifier, size, chunk_index, chunk) VALUES (:identifier, :size, :chunk_index, :chunk)";

    private static final String RETRIEVE_QUERY = "SELECT size FROM Binarydata WHERE identifier = ? LIMIT 1;";

    private static final String DELETE_QUERY = "DELETE FROM Binarydata WHERE identifier = ?;";

    private static final String READ_ALL_QUERY = "SELECT chunk_index FROM Binarydata WHERE identifier = ?;";

    private static final String READ_RANGE_QUERY = "SELECT chunk_index FROM Binarydata WHERE identifier = ? and chunk_index >= :start and chunk_index <= :end;";

    private static final String READ_CHUNK_QUERY = "SELECT chunk FROM Binarydata WHERE identifier = ? and chunk_index = :chunk_index;";

    final PreparedStatement deleteStatement, insertStatement, retrieveStatement, readRangeStatement, readStatement,
                    readChunkStatement;

    @Inject
    public BinaryQueryContext(Session session, @BinaryReadConsistency ConsistencyLevel readConsistency,
                    @BinaryReadConsistency ConsistencyLevel writeConsistency) {
        super(session);
        this.insertStatement = session.prepare(INSERT_QUERY).setConsistencyLevel(writeConsistency);
        this.deleteStatement = session.prepare(DELETE_QUERY).setConsistencyLevel(writeConsistency);
        this.retrieveStatement = session.prepare(RETRIEVE_QUERY).setConsistencyLevel(readConsistency);
        this.readStatement = session.prepare(READ_ALL_QUERY).setConsistencyLevel(readConsistency);
        this.readRangeStatement = session.prepare(READ_RANGE_QUERY).setConsistencyLevel(readConsistency);
        this.readChunkStatement = session.prepare(READ_CHUNK_QUERY).setConsistencyLevel(readConsistency);
    }
}