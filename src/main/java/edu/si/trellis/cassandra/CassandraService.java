package edu.si.trellis.cassandra;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.trellisldp.api.RuntimeTrellisException;

abstract class CassandraService {
    
    protected <T> Optional<T> resynchronize(CompletableFuture<T> from) {
        // TODO https://github.com/trellis-ldp/trellis/issues/148
        try {
            return Optional.of(from.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }
}
