
CREATE KEYSPACE IF NOT EXISTS trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE trellis;

-- Main data tables, including containment

CREATE TABLE IF NOT EXISTS mutabledata (identifier text PRIMARY KEY, quads text, interactionModel text,
    hasAcl boolean, binaryIdentifier text, mimeType text, size bigint, container text, modified timestamp);

CREATE TABLE IF NOT EXISTS immutabledata (identifier text PRIMARY KEY, quads text, modified timestamp);

CREATE TABLE IF NOT EXISTS binarydata (identifier text, chunk_index bigint, chunk blob,
    PRIMARY KEY (identifier, chunk_index)) WITH CLUSTERING ORDER BY (chunk_index ASC);

-- Indexes for containment

CREATE TABLE IF NOT EXISTS basiccontainment (identifier text, contained text, PRIMARY KEY (identifier, contained));
