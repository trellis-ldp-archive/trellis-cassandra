FROM openjdk:8-jdk
MAINTAINER Gregory Jansen <jansen@umd.edu>

# Add Maven dependencies (not shaded into the artifact; Docker-cached)
ADD target/webapp-*-hollow-thorntail.jar /webapp-hollow-thorntail.jar
ADD target/webapp-*.war /webapp.war

COPY docker_command.sh /command.sh
RUN chmod +x /command.sh

ARG GIT_COMMIT_ID_DESCRIBE
ARG GIT_ORIGIN_REMOTE_URL
LABEL git.remote.origin.url=${GIT_ORIGIN_REMOTE_URL}
LABEL git.commit.id.describe=${GIT_COMMIT_ID_DESCRIBE}


CMD ["/command.sh"]
