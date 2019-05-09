package edu.si.trellis;

import static com.datastax.driver.core.utils.UUIDs.unixTimestamp;
import static java.time.Instant.ofEpochMilli;
import static org.slf4j.LoggerFactory.getLogger;

import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MementoMutableRetrieve;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;

/**
 * A Memento of a {@link CassandraResource}.
 */
class CassandraMemento extends CassandraResource {

    private static final Logger log = getLogger(CassandraMemento.class);

    private final MementoMutableRetrieve mementoMutableRetrieve;

    CassandraMemento(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, IRI container,
                    Instant modified, UUID created, ImmutableRetrieve immutable,
                    MementoMutableRetrieve mementoMutableRetrieve) {
        super(id, ixnModel, hasAcl, binaryIdentifier, mimeType, container, modified, created, immutable, null, null);
        this.mementoMutableRetrieve = mementoMutableRetrieve;
    }

    @Override
    protected Stream<Quad> mutableQuads() {
        return mementoMutableRetrieve.execute(getIdentifier(), getModified());
    }

    @Override
    protected Stream<Quad> basicContainmentQuads() {
        return Stream.empty();
    }
}
