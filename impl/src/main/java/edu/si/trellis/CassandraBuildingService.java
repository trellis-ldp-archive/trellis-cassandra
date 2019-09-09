package edu.si.trellis;

import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;

import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;

abstract class CassandraBuildingService {
    
    private static final Logger log = getLogger(CassandraBuildingService.class);

    Resource parse(Row metadata) {
        if (metadata == null) return MISSING_RESOURCE;
        IRI id = metadata.get("identifier", IRI.class);
        log.debug("{} was found, computing metadata.", id);
        
        IRI ixnModel = metadata.get("interactionModel", IRI.class);
        log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
        boolean hasAcl = metadata.getBoolean("hasAcl");
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
