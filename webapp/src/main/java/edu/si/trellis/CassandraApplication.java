package edu.si.trellis;

import static org.apache.tamaya.Configuration.current;
import static org.apache.tamaya.Configuration.setCurrent;
import static org.slf4j.LoggerFactory.getLogger;

import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
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

/**
 * Basic JAX-RS {@link Application} to deploy Trellis with a Cassandra persistence implementation.
 *
 */
@ApplicationPath("/")
@ApplicationScoped
public class CassandraApplication extends Application {

    private static final Logger log = getLogger(CassandraApplication.class);

    @Inject
    private CassandraServiceBundler services;

    @Config(value = { "configurationFile", "TRELLIS_CONFIG_FILE" })
    private Optional<File> additionalConfigFile;

    @Inject
    @Config(value = { "configurationUrl", "TRELLIS_CONFIG_URL" })
    private Optional<URL> additionalConfigUrl;

    /**
     * Load in any additional configuration.
     */
    @PostConstruct
    public void importAdditionalConfig() {
        additionalConfigFile.map(this::toUrl).ifPresent(this::addConfig);
        additionalConfigUrl.ifPresent(this::addConfig);
        log.debug("Using system properties:");
        System.getProperties().forEach((k, v) -> log.debug("{} : {}", k, v));
        log.debug("Using ENV vars:");
        System.getenv().forEach((k, v) -> log.debug("{} : {}", k, v));
        log.debug("Using Tamaya configuration sources:");
        current().getContext().getPropertySources().forEach(s -> log.debug("{}", s));
        log.debug("Using Tamaya configuration:");
        current().getProperties().forEach((k, v) -> log.debug("{} : {}", k, v));
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
        return ImmutableSet.of(new TrellisHttpResource(services), new TrellisHttpFilter());
    }
}
