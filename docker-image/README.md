# Docker Image

This sub-project contains instructions for building a Docker machine image for the front-end Trellis-Cassandra service. It also includes a Docker compose
file that provides an example stack configuration, complete with Cassandra cluster and the ActiveMQ JMS broker. In a parallel folder you will find the docker-cassandra-init Docker build project. This one is a simple overlay on the Cassandra base image includes the Trellis Cassandra schema for database initialization by means of the Cassandra CQL client. This image is used to instantiate the database in the docker-compose.yml file.

## Build Instructions

By default neither of the Docker sub-projects are built by Maven. In order to build Docker images, you have to activate the 'docker' build profile.
Then your build will include the Docker image projects. The resulting machine images are added to the local Docker image repository.

```
$ mvn -P docker package
```

## Starting the stack

To start the stack, first make sure your images are present in the local docker repository:

```
$ docker image ls | grep trellis
```

Now execute the stack on your Docker Swarm:

```
$ docker stack deploy -c docker-compose.yml trellis
```

This will deploy the trellis stack to either a local or distributed swarm, complete with Cassandra initialization and front-end. You can observe the
trellis or cassandra logs using the docker service command:

```
$ docker service logs -f trellis_trellis-cassandra
```
