#!/bin/sh
java -Dswarm.http.port=8080 -Dorg.jboss.logging.provider=slf4j -Dlogback.configurationFile=/logback-docker.xml -Djava.net.preferIPv4Stack=true -jar webapp-hollow-thorntail.jar webapp.war
