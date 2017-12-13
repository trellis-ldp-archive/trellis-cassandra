package edu.si.trellis.cassandra;

import static java.util.Collections.emptyList;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static org.trellisldp.vocabulary.Trellis.PreferUserManaged;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.Resource;
import org.trellisldp.api.VersionRange;
import org.trellisldp.vocabulary.RDF;

import com.datastax.driver.mapping.annotations.Computed;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name = "rdfsource")
public class RDFSource implements Resource {

    @PartitionKey
    private IRI identifier;

    private Collection<IRI> types;

    private IRI ixnModel;

    private Dataset quads;
    
    public RDFSource() {}

    public RDFSource(IRI identifier, Dataset quads) {
        this.identifier = identifier;
        this.quads = quads;
        this.types = quads.stream(of(PreferUserManaged), null, RDF.type, null)
                .map(Quad::getObject)
                .map(IRI.class::cast)
                .collect(toList());
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    @Override
    public IRI getInteractionModel() {
        return ixnModel;
    }

    @Override
    public List<VersionRange> getMementos() {
        return emptyList();
    }

    @Computed("writetime()")
    @Override
    public Instant getModified() {
        return null;
    }

    @Override
    public Collection<IRI> getTypes() {
        return types;
    }

    @Override
    public Boolean hasAcl() {
        return false;
    }

    @Override
    public Stream<? extends Quad> stream() {
        return quads.stream();
    }
}
