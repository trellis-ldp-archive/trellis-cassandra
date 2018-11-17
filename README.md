# trellis-cassandra
The delicious vanilla of [Trellis LDP](https://github.com/trellis-ldp/trellis) laced with tasty chocolate [Apache Cassandra](https://cassandra.apache.org/) ribbons.

[![CircleCI](https://circleci.com/gh/ajs6f/trellis-cassandra/tree/master.svg?style=svg)](https://circleci.com/gh/ajs6f/trellis-cassandra/tree/master)

Use
```
mvn clean install
```
to build with a bundled Cassandra instance for testing. See Maven profiles in `webapp/pom.xml` for packaging options. Use
```
mvn -P-self-cassandra -Dcassandra.contactAddress=$NODE -Dcassandra.nativeTransportPort=$PORT clean install
```
 to use an non-bundled Cassandra cluster for testing, but be aware that you must load an appropriate schema yourself if you do this. Please find an example in `src/test/resources/load.cql`.

To configure for runtime, provide the location and port of a contact node in your Cassandra cluster. This can be done via environment properties or Java system properties (further methods coming soon). Use the names `cassandra.contactPort` and `cassandra.contactAddress` (subject to change < 1.0).


[![Travis-CI Status](https://travis-ci.org/ajs6f/trellis-cassandra.svg?branch=master)](https://travis-ci.org/ajs6f/trellis-cassandra)
