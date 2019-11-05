# Trellis/Cassandra

**Note: the code in this repository has been migrated to [trellis-extensions](https://github.com/trellis-ldp/trellis-extensions)**

Rich, delicious [TrellisLDP](https://github.com/trellis-ldp/trellis) ice cream laced with tasty [Apache Cassandra](https://cassandra.apache.org/) ribbons.

1. Clean separation of mutable and immutable (e.g. audit) RDF data in separate tables.
2. Immutable binary data.
2. RDF stored in the standard and transparent [N-Quads](https://www.w3.org/TR/n-quads/) serialization.
3. The renowned distribution and scaling characteristics of Apache Cassandra.

[![Travis-CI](https://travis-ci.com/trellis-ldp/trellis-cassandra.svg?branch=master)](https://travis-ci.com/trellis-ldp/trellis-cassandra)

Use
```
mvn clean install
```
to [build](https://github.com/trellis-ldp/trellis-cassandra/wiki/Building-and-running) with a build-provided Cassandra instance for testing. [Configure](https://github.com/trellis-ldp/trellis-cassandra/wiki/Configuration) to connect to your Cassandra cluster. Use Docker [to deploy](https://github.com/trellis-ldp/trellis-cassandra/wiki/Docker-machinery). Enjoy!
