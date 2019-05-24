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
to [build](wiki/Building-and-running) with a build-provided Cassandra instance for testing.
