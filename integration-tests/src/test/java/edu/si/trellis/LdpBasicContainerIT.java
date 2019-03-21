package edu.si.trellis;

import org.trellisldp.test.LdpBasicContainerTests;

class LdpBasicContainerIT extends IT implements LdpBasicContainerTests {

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
