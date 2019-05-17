#!/bin/sh

# Cassandra Options
OPTS="-Dcassandra.contactAddress=$CASSANDRA_CONTACT_ADDRESS"
OPTS="$OPTS -Dcassandra.contactPort=$CASSANDRA_CONTACT_PORT"
OPTS="$OPTS -Dcassandra.binaryReadConsistency=$CASSANDRA_BINARY_READ_CONSISTENCY"
OPTS="$OPTS -Dcassandra.binaryWriteConsistency=$CASSANDRA_BINARY_WRITE_CONSISTENCY"
OPTS="$OPTS -Dcassandra.rdfReadConsistency=$CASSANDRA_RDF_READ_CONSISTENCY"
OPTS="$OPTS -Dcassandra.rdfWriteConsistency=$CASSANDRA_RDF_WRITE_CONSISTENCY"



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

## JMS Options
OPTS="$OPTS -Dtrellis.jms.use.queue=$TRELLIS_JMS_USE_QUEUE"
OPTS="$OPTS -Dtrellis.jms.queue=$TRELLIS_JMS_QUEUE_NAME"
OPTS="$OPTS -Dtrellis.jms.url=$TRELLIS_JMS_URL"
OPTS="$OPTS -Dtrellis.jms.username=$TRELLIS_JMS_USERNAME"
OPTS="$OPTS -Dtrellis.jms.password=$TRELLIS_JMS_PASSWORD"

java $OPTS -jar webapp-hollow-thorntail.jar webapp.war
