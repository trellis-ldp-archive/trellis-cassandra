
CREATE KEYSPACE Trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE Trellis;

CREATE TABLE Metadata
(identifier text PRIMARY KEY, interactionModel text, hasAcl boolean, binaryIdentifier text, 
mimeType text, size bigint, parent text);
                        
CREATE TABLE Mutabledata (identifier text PRIMARY KEY, quads text);

CREATE TABLE Immutabledata (identifier text PRIMARY KEY, quads text);