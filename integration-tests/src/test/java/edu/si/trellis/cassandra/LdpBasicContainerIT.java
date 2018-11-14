package edu.si.trellis.cassandra;

import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;
import org.trellisldp.test.LdpBasicContainerTests;

public class LdpBasicContainerIT extends IT implements LdpBasicContainerTests {

    private static final Logger log = getLogger(LdpBasicContainerIT.class);

    private String container;

    @Override
    public void setContainerLocation(final String location) {
        log.debug("Container location set to: {}", location);
        container = location;
    }

    @Override
    public String getContainerLocation() {
        log.debug("Container location is: {}", container);
        return container;
    }
}
