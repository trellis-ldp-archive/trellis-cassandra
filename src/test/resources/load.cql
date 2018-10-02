
CREATE KEYSPACE IF NOT EXISTS trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE trellis;

CREATE TABLE IF NOT EXISTS mutabledata (identifier text PRIMARY KEY, quads text, interactionModel text,
    hasAcl boolean, binaryIdentifier text, mimeType text, size bigint, container text);

CREATE TABLE IF NOT EXISTS immutabledata (identifier text PRIMARY KEY, quads text);

CREATE TABLE IF NOT EXISTS binarydata (identifier text, chunk_index bigint, chunk blob,
    PRIMARY KEY (identifier, chunk_index)) WITH CLUSTERING ORDER BY (chunk_index ASC);
