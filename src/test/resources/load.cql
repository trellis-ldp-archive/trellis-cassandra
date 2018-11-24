
CREATE KEYSPACE IF NOT EXISTS trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE trellis;

-- Main data tables

CREATE TABLE IF NOT EXISTS mutabledata (identifier text, quads text, interactionModel text,
    hasAcl boolean, binaryIdentifier text, mimeType text, size bigint, container text, created timeuuid,
    modified timestamp, createdSeconds timestamp, PRIMARY KEY ((identifier), created))
    WITH CLUSTERING ORDER BY (created DESC);

CREATE TABLE IF NOT EXISTS immutabledata (identifier text, quads text, created timestamp,
    PRIMARY KEY ((identifier), created)) WITH CLUSTERING ORDER BY (created DESC);

CREATE TABLE IF NOT EXISTS binarydata (identifier text, chunk_index int, chunk blob,
    PRIMARY KEY (identifier, chunk_index)) WITH CLUSTERING ORDER BY (chunk_index ASC);

-- Index for basic containment

CREATE MATERIALIZED VIEW IF NOT EXISTS basiccontainment AS
       SELECT identifier, container, created FROM mutabledata
       WHERE identifier IS NOT NULL AND container IS NOT NULL AND created IS NOT NULL
       PRIMARY KEY ((container), identifier, created);

