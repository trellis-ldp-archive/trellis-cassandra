package edu.si.trellis;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static org.apache.tamaya.Configuration.current;
import static org.apache.tamaya.Configuration.setCurrent;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.http.core.HttpConstants.CONFIG_HTTP_PUT_UNCONTAINED;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.apache.tamaya.format.ConfigurationFormats;
import org.apache.tamaya.inject.api.Config;
import org.apache.tamaya.spi.PropertySource;
import org.slf4j.Logger;
import org.trellisldp.http.TrellisHttpFilter;
import org.trellisldp.http.TrellisHttpResource;
import org.trellisldp.webdav.TrellisWebDAV;
import org.trellisldp.webdav.TrellisWebDAVRequestFilter;
import org.trellisldp.webdav.TrellisWebDAVResponseFilter;

/**
 * Basic JAX-RS {@link Application} to deploy Trellis with a Cassandra persistence implementation.
 *
 */
@ApplicationPath("/")
@ApplicationScoped
public class CassandraApplication extends Application {

    private static final Logger log = getLogger(CassandraApplication.class);

    @Config(key = "configurationFile", alternateKeys = { "TRELLIS_CONFIG_FILE" })
    private Optional<File> additionalConfigFile;

    @Inject
    @Config(key = "configurationUrl", alternateKeys = { "TRELLIS_CONFIG_URL" })
    private Optional<URL> additionalConfigUrl;

    @Inject
    private TrellisHttpResource ldpHttpResource;

    @Inject
    private TrellisWebDAV webDav;

    @Inject
    private TrellisWebDAVRequestFilter webDavRequestFilter;

    @Inject
    private TrellisHttpFilter httpFilter;

    @Inject
    private TrellisWebDAVResponseFilter webDavResponseFilter;

    private Set<Object> singletons;

    @PostConstruct
    private void initialize() {
        importAdditionalConfig();
        List<Object> list = asList(ldpHttpResource, httpFilter, webDav, webDavRequestFilter, webDavResponseFilter);
        this.singletons = unmodifiableSet(new HashSet<>(list));
        log.info("JAX-RS components assembled.");
    }

    /**
     * Load in any additional configuration.
     */
    public void importAdditionalConfig() {
        // we require contained PUT because we use the Trellis WebDAV module, which requires it
        System.setProperty(CONFIG_HTTP_PUT_UNCONTAINED, "false");
        additionalConfigFile.map(this::toUrl).ifPresent(this::addConfig);
        additionalConfigUrl.ifPresent(this::addConfig);
        log.debug("Using system properties:");
        log(System.getProperties());
        log.debug("Using ENV vars:");
        log(System.getenv());
        log.debug("Using Tamaya configuration sources:");
        current().getContext().getPropertySources().stream().map(PropertySource::getName).forEach(log::debug);
        log.debug("Using Tamaya configuration:");
        log(current().getProperties());
        log.info("Additional configuration imported.");
    }

    private static <K, V> void log(Map<K, V> config) {
        config.forEach((k, v) -> log.debug("{} : {}", k, v));
    }

    private URL toUrl(File f) {
        try {
            return f.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void addConfig(URL url) {
        try {
            log.info("Adding additional config from: {}", url.toURI());
            PropertySource propSource = ConfigurationFormats.getInstance().createPropertySource(url);
            setCurrent(current().toBuilder().addPropertySources(propSource).build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}