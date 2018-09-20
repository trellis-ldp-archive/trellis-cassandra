A TrellisLDP application using Trellis/Cassandra for persistence.

`mvn clean install` to build, see Maven profiles in `pom.xml` for packaging options.

To configure, provide the location and port of a node in your Cassandra cluster. This can be done via environment properties or Java system properties (further methods coming soon). Use the names `contactPort` and `contactAddress` (subject to change < 1.0).


See [Trellis-Cassandra](https://github.com/ajs6f/trellis-cassandra) and [Trellis](https://github.com/trellis-ldp/trellis).
