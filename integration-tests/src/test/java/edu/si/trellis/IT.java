package edu.si.trellis;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.client.ClientBuilder.newBuilder;

import javax.ws.rs.client.Client;

abstract class IT {

    private static Client client = newBuilder().connectTimeout(2, MINUTES).build();
    private static final String trellisUri = "http://localhost:" + getInteger("trellis.port") + "/";
    private String resourceLocation;
    private String binaryLocation;

    public synchronized Client getClient() {
        return client;
    }

    public String getBaseURL() {
        return trellisUri;
    }

    public void setResourceLocation(String location) {
        this.resourceLocation = location;
    }

    public String getResourceLocation() {
        return resourceLocation;
    }

    public String getBinaryLocation() {
        return binaryLocation;
    }

    public void setBinaryLocation(String location) {
        this.binaryLocation = location;
    
    }
}
