package edu.si.trellis.cassandra;

import static java.lang.Integer.parseInt;

import edu.si.trellis.cassandra.CassandraBinaryService.MaxChunkSize;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.tamaya.inject.api.Config;
import org.trellisldp.api.*;
import org.trellisldp.io.JenaIOService;

/**
 * Use to supply injected components for a Trellis application.
 *
 */
@ApplicationScoped
public class CassandraServiceBundler implements ServiceBundler {

    @Inject
    private AuditService auditService;

    @Inject
    private CassandraResourceService resourceService;

    @Inject
    private CassandraBinaryService binaryService;

    @Inject
    private AgentService agentService;

    @Inject
    private NamespaceService namespaceService;

    @Produces
    @ApplicationScoped
    private IOService ioService;

    @Inject
    private EventService eventService;

    @Inject
    private CacheService<String, String> cacheService;

    @PostConstruct
    void init() {
        this.ioService = new JenaIOService(namespaceService, null, cacheService, "", "");
    }

    @Override
    public AgentService getAgentService() {
        return agentService;
    }

    @Override
    public ResourceService getResourceService() {
        return resourceService;
    }

    @Override
    public IOService getIOService() {
        return ioService;
    }

    @Override
    public BinaryService getBinaryService() {
        return binaryService;
    }

    @Override
    public AuditService getAuditService() {
        return auditService;
    }

    @Override
    public MementoService getMementoService() {
        return resourceService;
    }

    @Override
    public EventService getEventService() {
        return eventService;
    }
}
