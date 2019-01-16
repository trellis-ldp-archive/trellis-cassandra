package edu.si.trellis;

import org.trellisldp.api.RuntimeTrellisException;

/**
 * Thrown to indicate that application initialization was interrupted.
 */
public class InterruptedStartupException extends RuntimeTrellisException {

    private static final long serialVersionUID = 1L;

    public InterruptedStartupException(String message, InterruptedException cause) {
        super(message, cause);
    }
}
