package edu.si.trellis.cassandra;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;

import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name = "resource")
public class CassandraResource implements Resource {

    final IRI identifier;

    public CassandraResource(IRI identifier) {
        this.identifier = identifier;

    }

    @PartitionKey
    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getInteractionModel() {
        return null;
    }

    @Override
    public List<VersionRange> getMementos() {
        throw new UnsupportedOperationException();
    }

    @Computed("writetime()")
    @Override
    public Instant getModified() {
        return null;
    }

    @Override
    public Collection<IRI> getTypes() {
        return null;
    }

    @Override
    public Boolean hasAcl() {
        return true;
    }

    @Override
    public Stream<? extends Quad> stream() {
        return null;
    }

}
