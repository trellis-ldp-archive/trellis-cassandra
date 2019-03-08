package edu.si.trellis;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import javax.inject.Qualifier;

/**
 * The timeout for making a connection to Cassandra, in ms.
 */
@Documented
@Retention(RUNTIME)
@Qualifier
public @interface ConnectionTimeout {}
