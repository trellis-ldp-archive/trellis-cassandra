# Trellis/Cassandra
Rich, delicious [TrellisLDP](https://github.com/trellis-ldp/trellis) ice cream laced with tasty [Apache Cassandra](https://cassandra.apache.org/) ribbons.

1. Clean separation of mutable and immutable (e.g. audit) RDF data in separate tables.
2. Immutable binary data.
2. RDF stored in the standard and transparent [N-Quads](https://www.w3.org/TR/n-quads/) serialization.
3. The renowned distribution and scaling characteristics of Apache Cassandra.

[![CircleCI](https://circleci.com/gh/trellis-ldp/trellis-cassandra/tree/master.svg?style=svg)](https://circleci.com/gh/trellis-ldp/trellis-cassandra/tree/master)
[![Travis-CI](https://travis-ci.org/trellis-ldp/trellis-cassandra.svg?branch=master)](https://travis-ci.org/trellis-ldp/trellis-cassandra)

Use
```
mvn clean install
```
to build with a bundled Cassandra instance for testing. See Maven profiles for packaging options. Use
```
mvn -Dcassandra.skip -Dcassandra.contactAddress=$NODE -Dcassandra.nativeTransportPort=$PORT clean install
```
 to use an non-bundled Cassandra cluster for testing, but be aware that you must load an appropriate schema yourself into the `Trellis` keyspace if you do this. Please find an example in [`src/test/resources/load.cql`](src/test/resources/load.cql).

You can launch the built application (found in `webapp/target`) via an invocation:
```
java $OPTS -jar webapp-$version-thorntail.jar webapp-$version.war
```
with `OPTS` set to whatever runtime properties for configuration you may require.

### Important Options

To configure the connection to Cassandra, you must provide the location and port of an initial contact node in your Cassandra cluster. This cluster must be configured (by some other means) with a minimal schema in the `Trellis` keyspace such as is shown in `src/test/resources/load.cql`. The connection can be configured via environment properties (or Java system properties). Use the names `CASSANDRA_CONTACT_PORT`(`cassandra.contactPort`) and `CASSANDRA_CONTACT_ADDRESS`(`cassandra.contactAddress`) (subject to change < 1.0). These default to `localhost` and `9042`. Additionally, you may configure the size (in bytes) of chunk used for binary storage as `CASSANDRA_MAX_CHUNK_SIZE`(`cassandra.maxChunkSize`).

#### Logging
Trellis/Cassandra uses Logback for logging. To enable and configure logging, configure Logback via:
```
-Dorg.jboss.logging.provider=slf4j  -Dlogback.configurationFile=/your/logback/config
```
Because Trellis/Cassandra lifts `java.util.logging` over SLF4j, any Logback configuration [should use Logback's `LevelChangePropagator`](https://logback.qos.ch/manual/configuration.html#LevelChangePropagator). You can do this in an XML Logback configuration file via:
```
  <contextListener class="ch.qos.logback.classic.jul.LevelChangePropagator"/>
```
#### Containerization
In some containerized deployments, you may receive an error like `java.net.SocketException: Protocol family unavailable`, which indicates that the appplication is trying to bind to an IPv6 port. You can prevent this if needed via
```
-Djava.net.preferIPv4Stack=true`
```
#### Connecting to Cassandra
To configure the connection to Cassandra, you must provide the location and port of an initial contact node in your Cassandra cluster. This can be done via environment properties (or Java system properties). Use the names `CASSANDRA_CONTACT_PORT`(`cassandra.contactPort`) and `CASSANDRA_CONTACT_ADDRESS`(`cassandra.contactAddress`) (subject to change < 1.0). These default to `localhost` and `9042`. Additionally, you may configure the size (in bytes) of chunk used for binary storage as `CASSANDRA_MAX_CHUNK_SIZE`(`cassandra.maxChunkSize`).

It is also possible to adjust consistency settings for read and write for binary and RDF data, all independently. The configuration keys are as follows:

| Data category | READ | WRITE |
| ------------- | ---- | ----- |
| Binary | `CASSANDRA_BINARY_READ_CONSISTENCY` (`cassandra.binaryReadConsistency`) | `CASSANDRA_BINARY_WRITE_CONSISTENCY` (`cassandra.binaryWriteConsistency`) |
| RDF | `CASSANDRA_RDF_READ_CONSISTENCY` (`cassandra.rdfReadConsistency`) |  `CASSANDRA_RDF_WRITE_CONSISTENCY` (`cassandra.rdfWriteConsistency`) |

and values are drawn from the usual [Cassandra options](https://cassandra.apache.org/doc/latest/architecture/dynamo.html#tunable-consistency). The default value for each consistency level is `ONE`.
## Persistent configuration
You may also use a JSON document for these settings. Use `TRELLIS_CONFIG_FILE` (`configurationFile`) to use a file or `TRELLIS_CONFIG_URL` (`configurationUrl`) to use a document loaded from an arbitrary URL. The document should be a simple object with keys named as system properties shown above (`cassandra.contactPort`, `cassandra.contactAddress`, etc.).
## Special HTTP headers
Trellis/Cassandra offers some special HTTP request headers to enable customization of workflows and persistence. The most important is `Cassandra-Chunk-Size`, which, when applied to a request to persist binary data, will set the size of chunk (in bytes) that is used to chunk out the bitstream. This is particularly important if you expect to distribute computation across your Cassandra cluster. You will want to set this value high enough that a given distributed task will be able to complete on any resource to which it is applied without requiring data from more than one node.
## Application management API/console
Trellis-Cassandra provides the [Thorntail/WildFly application management API and console](http://docs.wildfly.org/15/Admin_Guide.html#web-management-interface). The API is normally available, but set Java system property `thorntail.management.http-interface-management-interface.console-enabled` to `true` to use the console. Use [Thorntail configuration settings](https://docs.thorntail.io/2.3.0.Final/#_management) to adjust the API and console.
