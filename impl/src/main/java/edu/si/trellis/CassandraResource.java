package edu.si.trellis;

import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.getSuperclassOf;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.Resource;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private final IRI identifier, container, interactionModel;

    final boolean hasAcl, isContainer;

    private final Instant modified;

    private final BinaryMetadata binary;

    private final Dataset dataset;

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, IRI container,
                    Instant modified, Dataset dataset) {
        this.identifier = id;
        this.interactionModel = ixnModel;
        this.isContainer = Container.equals(getInteractionModel())
                        || Container.equals(getSuperclassOf(getInteractionModel()));
        this.hasAcl = hasAcl;
        this.container = container;
        log.trace("Resource is {}a container.", !isContainer ? "not " : "");
        this.modified = modified;
        boolean isBinary = NonRDFSource.equals(getInteractionModel());
        this.binary = isBinary ? builder(binaryIdentifier).mimeType(mimeType).build() : null;
        log.trace("Resource is {}a NonRDFSource.", !isBinary ? "not " : "");
        this.dataset = dataset;
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    /**
     * @return a container for this resource
     */
    @Override
    public Optional<IRI> getContainer() {
        return Optional.ofNullable(container);
    }

    @Override
    public IRI getInteractionModel() {
        return interactionModel;
    }

    @Override
    public Instant getModified() {
        return modified;
    }

    @Override
    public boolean hasAcl() {
        return hasAcl;
    }

    @Override
    public Optional<BinaryMetadata> getBinaryMetadata() {
        return Optional.ofNullable(binary);
    }

    @Override
    public Dataset dataset() {
        return dataset;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<Quad> stream() {
        return (Stream<Quad>) dataset.stream();
    }
}
