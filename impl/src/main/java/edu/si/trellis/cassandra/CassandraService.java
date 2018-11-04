package edu.si.trellis.cassandra;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.trellisldp.api.RuntimeTrellisException;

abstract class CassandraService {

    private static final Logger log = getLogger(CassandraService.class);

    protected Session session;

    /**
     * Same-thread execution. TODO use a pool?
     */
    private final Executor executor = Runnable::run;

    public CassandraService(Session session) {
        this.session = session;
    }

    protected Session session() {
        return session;
    }

    protected CompletableFuture<Void> execute(Statement statement) {
        log.debug("Executing CQL statement: {}", statement);
        return read(statement).thenApply(r -> null);
    }

    protected CompletableFuture<ResultSet> read(Statement statement) {
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
        }, executor);
    }

    protected <T> Optional<T> resynchronize(CompletableFuture<T> from) {
        try {
            return Optional.of(from.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

}
