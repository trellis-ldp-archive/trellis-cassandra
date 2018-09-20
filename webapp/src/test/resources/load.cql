
CREATE KEYSPACE IF NOT EXISTS Trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE Trellis;

CREATE TABLE IF NOT EXISTS Metadata (identifier text PRIMARY KEY, interactionModel text,
    hasAcl boolean, binaryIdentifier text, mimeType text, size bigint, parent text);

CREATE TABLE IF NOT EXISTS Mutabledata (identifier text PRIMARY KEY, quads text);

CREATE TABLE IF NOT EXISTS Immutabledata (identifier text PRIMARY KEY, quads text);

CREATE TABLE IF NOT EXISTS Binarydata (identifier text, chunk_index bigint, chunk blob,
    PRIMARY KEY (identifier, chunk_index)) WITH CLUSTERING ORDER BY (chunk_index ASC);
