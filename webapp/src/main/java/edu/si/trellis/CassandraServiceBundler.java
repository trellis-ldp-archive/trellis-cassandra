package edu.si.trellis;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.trellisldp.api.*;
import org.trellisldp.http.core.EtagGenerator;
import org.trellisldp.http.core.ServiceBundler;
import org.trellisldp.http.core.TimemapGenerator;
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
    private CassandraMementoService mementoService;

    @Inject
    private CassandraBinaryService binaryService;

    @Inject
    private AgentService agentService;

    @Produces
    @ApplicationScoped
    private NamespaceService namespaceService = new NoopNamespaceService();

    @Produces
    @ApplicationScoped
    private IOService ioService;

    @Inject
    private EventService eventService;

    @Inject
    private CacheService<String, String> cacheService;

    @Inject
    private TimemapGenerator timemapGenerator;

    @Inject
    private EtagGenerator etagGenerator;

    @Inject
    private Instance<ConstraintService> constraintServices;

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
        return mementoService;
    }

    @Override
    public EventService getEventService() {
        return eventService;
    }

    @Override
    public EtagGenerator getEtagGenerator() {
        return etagGenerator;
    }

    @Override
    public TimemapGenerator getTimemapGenerator() {
        return timemapGenerator;
    }

    @Override
    public Iterable<ConstraintService> getConstraintServices() {
        return constraintServices;
    }
}
