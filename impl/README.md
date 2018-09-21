Trellis/Cassandra is a persistence module for the [Trellis LDP](https://github.com/trellis-ldp/trellis) platform. It stores information in an [Apache Cassandra](https://cassandra.apache.org/) cluster that you provide to it. Features include:

1. Clean separation of mutable and immutable (e.g. audit) data in separate tables.
2. Storage of RDF in the standard and easily-parsed [N-Quads](https://www.w3.org/TR/n-quads/) serialization.
3. The renowned distribution and scaling characteristics of Apache Cassandra.