package edu.si.trellis.cassandra;

import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.jena.JenaRDF;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;

public abstract class CachingRdfTermCodec<T extends RDFTerm> extends TypeCodec<T> {


    protected CachingRdfTermCodec(Class<T> javaClass) {
        super(DataType.text(), javaClass);
    }

}
