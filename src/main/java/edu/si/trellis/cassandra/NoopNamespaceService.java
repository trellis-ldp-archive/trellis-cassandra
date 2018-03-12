package edu.si.trellis.cassandra;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.trellisldp.api.NamespaceService;

public class NoopNamespaceService implements NamespaceService {

    @Override
    public Map<String, String> getNamespaces() {
        return Collections.emptyMap();
    }

    @Override
    public Optional<String> getNamespace(String prefix) {
        return Optional.empty();
    }

    @Override
    public Optional<String> getPrefix(String namespace) {
        return Optional.empty();
    }

    @Override
    public Boolean setPrefix(String prefix, String namespace) {
        return false;
    }

}
