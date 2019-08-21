package edu.si.trellis;

import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.time.Instant;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;

abstract class CassandraBuildingService {

    Resource parse(ResultSet rows, Logger log, IRI id) {
        final Row metadata;
        if ((metadata = rows.one()) == null) {
            log.debug("{} was not found.", id);
            return MISSING_RESOURCE;
        }

        log.debug("{} was found, computing metadata.", id);
        IRI ixnModel = metadata.get("interactionModel", IRI.class);
        log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
        boolean hasAcl = metadata.getBool("hasAcl");
        log.debug("Found hasAcl = {} for resource {}", hasAcl, id);
        IRI binaryId = metadata.get("binaryIdentifier", IRI.class);
        log.debug("Found binaryIdentifier = {} for resource {}", binaryId, id);
        String mimeType = metadata.getString("mimetype");
        log.debug("Found mimeType = {} for resource {}", mimeType, id);
        IRI container = metadata.get("container", IRI.class);
        log.debug("Found container = {} for resource {}", container, id);
        Instant modified = metadata.get("modified", Instant.class);
        log.debug("Found modified = {} for resource {}", modified, id);
        Dataset dataset = metadata.get("quads", Dataset.class);
        log.debug("Found dataset = {} for resource {}", dataset, id);
        
        return new CassandraResource(id, ixnModel, hasAcl, binaryId, mimeType, container, modified, dataset);
    }
}
