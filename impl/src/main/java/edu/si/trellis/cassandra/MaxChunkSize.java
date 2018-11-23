package edu.si.trellis.cassandra;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import javax.inject.Qualifier;

/**
 * The maximum size of any chunk in this service.
 */
@Documented
@Retention(RUNTIME)
@Qualifier
public @interface MaxChunkSize {

    /**
     * Use 1MB for default max chunk size.
     */
    public static final int DEFAULT_MAX_CHUNK_SIZE = 1024 * 1024;

}