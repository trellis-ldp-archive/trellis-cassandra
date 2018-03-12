package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.RDFTerm;

public class RDFTermCodec extends CachingRdfTermCodec<RDFTerm> {

    /**
     * Singleton instance.
     */
    public static final RDFTermCodec rdfTermCodec = new RDFTermCodec();

    protected RDFTermCodec() {
        super(RDFTerm.class);
    }

}
