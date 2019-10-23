package edu.si.trellis;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import javax.inject.Qualifier;

/**
 * Marks a {@link ConsistencyLevel} used for writing mutable metadata.
 */
@Documented
@Retention(RUNTIME)
@Qualifier
public @interface MutableWriteConsistency {}
