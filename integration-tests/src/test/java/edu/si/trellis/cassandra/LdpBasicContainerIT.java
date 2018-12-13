package edu.si.trellis.cassandra;

import org.trellisldp.test.LdpBasicContainerTests;

public class LdpBasicContainerIT extends IT implements LdpBasicContainerTests {

    private String container;

    @Override
    public void setContainerLocation(final String location) {
        container = location;
    }

    @Override
    public String getContainerLocation() {
        return container;
    }
}
