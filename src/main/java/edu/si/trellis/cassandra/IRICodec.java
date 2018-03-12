package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.IRI;

/**
 * (De)serializes Commons RDF {@link IRI}s into/out of Cassandra fields.
 * 
 * @author ajs6f
 *
 */
public class IRICodec extends CachingRdfTermCodec<IRI> {

    /**
     * Singleton instance.
     */
    public static final IRICodec iriCodec = new IRICodec();

    /**
     * Default constructor.
     */
    public IRICodec() {
        super(IRI.class);
    }
}
