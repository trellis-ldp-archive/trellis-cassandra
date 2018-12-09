package edu.si.trellis.cassandra;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;
import org.trellisldp.api.RuntimeTrellisException;

abstract class CassandraService {

    private static final Logger log = getLogger(CassandraService.class);

    protected static <T> Function<Row, T> getFieldAs(String k, Class<T> klass) {
        return row -> row.get(k, klass);
    }

    protected Session session;

    private ConsistencyLevel readConsistency, writeConsistency;

    protected ConsistencyLevel readConsistency() {
        return readConsistency;
    }

    protected ConsistencyLevel writeConsistency() {
        return writeConsistency;
    }

    protected final Executor workers = newCachedThreadPool();

    protected CassandraService(Session session, ConsistencyLevel readCons, ConsistencyLevel writeCons) {
        this.session = session;
        this.readConsistency = readCons;
        this.writeConsistency = writeCons;
    }

    protected Session session() {
        return session;
    }

    protected CompletableFuture<Void> executeAndDone(Statement statement) {
        log.debug("Executing CQL statement: {}", statement);
        return execute(statement).thenAccept(r -> log.debug("Executed CQL statement: {}", statement));
    }

    protected CompletableFuture<ResultSet> execute(Statement statement) {
        return translate(session().executeAsync(statement));
    }

    protected <T> CompletableFuture<T> translate(ListenableFuture<T> result) {
        return supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                // we don't know that persistence failed but we can't assume that it succeeded
                log.error("Error in persistence!", e.getCause());
                throw new CompletionException(e.getCause());
            }
        }, workers);
    }
}
