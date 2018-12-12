package edu.si.trellis.cassandra;

import com.datastax.driver.core.Row;

import java.util.function.Function;

abstract class CassandraService {

    protected static <T> Function<Row, T> getFieldAs(String k, Class<T> klass) {
        return row -> row.get(k, klass);
    }

}
