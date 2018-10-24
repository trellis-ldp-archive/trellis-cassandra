package edu.si.trellis.cassandra;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.trellisldp.api.*;
import org.trellisldp.io.JenaIOService;

/**
 * Use to supply injected components for a Trellis application.
 *
 */
@ApplicationScoped
public class InjectedServiceBundler implements ServiceBundler {

    @Inject
    private MementoService mementoService;

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
    public void init() {
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
        return mementoService;
    }

    @Override
    public EventService getEventService() {
        return eventService;
    }
}
