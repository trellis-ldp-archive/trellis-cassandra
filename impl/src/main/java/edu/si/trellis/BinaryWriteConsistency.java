package edu.si.trellis;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.datastax.driver.core.ConsistencyLevel;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import javax.inject.Qualifier;

/**
 * Marks a {@link ConsistencyLevel} used for writing binaries.
 */
@Documented
@Retention(RUNTIME)
@Qualifier
public @interface BinaryWriteConsistency {}
