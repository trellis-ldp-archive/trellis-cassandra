package edu.si.trellis;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import javax.inject.Qualifier;

/**
 * The default size of any chunk in this service.
 */
@Documented
@Retention(RUNTIME)
@Qualifier
public @interface DefaultChunkSize {}