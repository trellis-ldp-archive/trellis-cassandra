
CREATE KEYSPACE Trellis WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE Trellis;

CREATE TABLE Metadata (identifier text PRIMARY KEY, interactionModel text, hasAcl boolean, parent text, modified timestamp);
                        
CREATE TABLE Mutabledata (identifier text PRIMARY KEY, graph text, subject text, predicate text, object text, modified timestamp);
                           
CREATE TABLE Immutabledata (identifier text PRIMARY KEY, graph text, subject text, predicate text, object text, modified timestamp);