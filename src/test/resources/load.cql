
CREATE KEYSPACE IF NOT EXISTS trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE trellis;

-- Main data tables

CREATE TABLE IF NOT EXISTS mutabledata (identifier text, quads text, interactionModel text,
    hasAcl boolean, binaryIdentifier text, mimeType text, size bigint, container text, creation timestamp,
    modified timestamp, PRIMARY KEY ((identifier), creation)) WITH CLUSTERING ORDER BY (creation DESC);

CREATE TABLE IF NOT EXISTS immutabledata (identifier text, quads text, modified timestamp,
    PRIMARY KEY ((identifier), modified)) WITH CLUSTERING ORDER BY (modified DESC);

CREATE TABLE IF NOT EXISTS binarydata (identifier text, chunk_index bigint, chunk blob,
    PRIMARY KEY (identifier, chunk_index)) WITH CLUSTERING ORDER BY (chunk_index ASC);

-- Index for basic containment

CREATE MATERIALIZED VIEW IF NOT EXISTS basiccontainment AS
       SELECT identifier, container, creation FROM mutabledata
       WHERE identifier IS NOT NULL AND container IS NOT NULL AND creation IS NOT NULL
       PRIMARY KEY ((container), identifier, creation);

