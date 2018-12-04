FROM openjdk:8-jdk
MAINTAINER Gregory Jansen <jansen@umd.edu>

# Add Maven dependencies (not shaded into the artifact; Docker-cached)
ADD target/webapp-*-hollow-thorntail.jar /webapp-hollow-thorntail.jar
ADD target/webapp-*.war /webapp.war

COPY docker_command.sh /command.sh
RUN chmod +x /command.sh
COPY logback-docker.xml /logback-docker.xml
RUN mkdir /logs

ARG GIT_COMMIT_ID_DESCRIBE
ARG GIT_ORIGIN_REMOTE_URL
LABEL git.url=${GIT_ORIGIN_REMOTE_URL}
LABEL git.commit=${GIT_COMMIT_ID_DESCRIBE}


CMD ["/command.sh"]
