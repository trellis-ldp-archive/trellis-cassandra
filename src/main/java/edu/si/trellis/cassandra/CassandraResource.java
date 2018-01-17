package edu.si.trellis.cassandra;

import static java.util.Collections.emptyList;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;

import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;

@Table(name = "Resource", keyspace = "Trellis")
public class CassandraResource implements Resource {

    @PartitionKey
    public IRI identifier;

    public IRI interactionModel;

    public Dataset quads;

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

    @Override
    public IRI getInteractionModel() {
        return interactionModel;
    }

    @Transient
    @Override
    public List<VersionRange> getMementos() {
        return emptyList();
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
