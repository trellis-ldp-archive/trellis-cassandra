#!/bin/bash

# HTTP Server Options
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.max-post-size=1000000000000"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.enable-http2=true"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.max-connections=2000"
OPTS="$OPTS -Dswarm.http.port=8080"
OPTS="$OPTS -Djava.net.preferIPv4Stack=true"
OPTS="$OPTS -Dswarm.ajp.enable=false"

# Logging Options
OPTS="$OPTS -Dorg.jboss.logging.provider=slf4j"
OPTS="$OPTS -Dlogback.configurationFile=/logback.xml"

java $OPTS -jar webapp-hollow-thorntail.jar webapp.war
