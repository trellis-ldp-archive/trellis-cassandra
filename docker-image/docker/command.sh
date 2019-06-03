#!/bin/sh

# HTTP Server Options
OPTS=" -Dswarm.undertow.servers.default-server.http-listeners.default.max-post-size=1000000000000"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.enable-http2=true"
OPTS="$OPTS -Dswarm.undertow.servers.default-server.http-listeners.default.max-connections=2000"
OPTS="$OPTS -Dswarm.http.port=8080"
OPTS="$OPTS -Djava.net.preferIPv4Stack=true"
OPTS="$OPTS -Dswarm.ajp.enable=false"

# Logging Options
OPTS="$OPTS -Dorg.jboss.logging.provider=slf4j"
OPTS="$OPTS -Dlogback.configurationFile=/logback.xml"
OPTS="$OPTS -Dtrellis.auth.basic.credentials=$TRELLIS_AUTH_BASIC_CREDENTIALS"

## JMS Options
OPTS="$OPTS -Dtrellis.jms.use.queue=$TRELLIS_JMS_USE_QUEUE"
OPTS="$OPTS -Dtrellis.jms.queue=$TRELLIS_JMS_QUEUE_NAME"
OPTS="$OPTS -Dtrellis.jms.url=$TRELLIS_JMS_URL"
OPTS="$OPTS -Dtrellis.jms.username=$TRELLIS_JMS_USERNAME"
OPTS="$OPTS -Dtrellis.jms.password=$TRELLIS_JMS_PASSWORD"

java $OPTS -jar webapp-hollow-thorntail.jar webapp.war
