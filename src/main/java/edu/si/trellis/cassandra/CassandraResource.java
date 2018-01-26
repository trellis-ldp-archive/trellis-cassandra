package edu.si.trellis.cassandra;

import java.time.Instant;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.Resource;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;

@Table(name = "Resource", keyspace = "Trellis")
public class CassandraResource implements Resource {

    @PartitionKey
    public IRI identifier;

    public IRI interactionModel;

    public Dataset quads;

    public IRI parent;

    public Instant modified;

    public CassandraResource() {}

    public CassandraResource(IRI identifier, IRI ixnModel, Dataset quads) {
        this.identifier = identifier;
        this.quads = quads;
        this.interactionModel = ixnModel;
        this.modified = Instant.now();
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    public IRI parent() {
        return parent;
    }

    @Override
    public IRI getInteractionModel() {
        return interactionModel;
    }

    @Override
    public Instant getModified() {
        return modified;
    }

    @Transient
    @Override
    public Boolean hasAcl() {
        return false;
    }

    @Transient
    @Override
    public Stream<? extends Quad> stream() {
        return quads.stream();
    }
}
