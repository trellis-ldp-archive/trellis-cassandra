#!/bin/sh
NUM_CLIENT_THREADS=2000
OPTS="-Dcassandra.contactAddress=$CASSANDRA_CONTACT_ADDRESS"
OPTS="$OPTS -Dcassandra.contactPort=$CASSANDRA_CONTACT_PORT"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.max-post-size=1000000000000"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.enable-http2=true"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.max-connections=$NUM_CLIENT_THREADS"
OPTS="$OPTS -Dswarm.http.port=8080"
OPTS="$OPTS -Dorg.jboss.logging.provider=slf4j"
OPTS="$OPTS -Dlogback.configurationFile=/logback-docker.xml"
OPTS="$OPTS -Djava.net.preferIPv4Stack=true"
OPTS="$OPTS -Dswarm.ajp.enable=false"
java $OPTS -jar webapp-hollow-thorntail.jar webapp.war
