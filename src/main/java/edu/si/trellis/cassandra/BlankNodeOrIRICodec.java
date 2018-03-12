package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.BlankNodeOrIRI;

public class BlankNodeOrIRICodec extends CachingRdfTermCodec<BlankNodeOrIRI> {

    /**
     * Singleton instance.
     */
    public static final BlankNodeOrIRICodec blankNodeOrIRICodec = new BlankNodeOrIRICodec();

    protected BlankNodeOrIRICodec() {
        super(BlankNodeOrIRI.class);
    }

}
