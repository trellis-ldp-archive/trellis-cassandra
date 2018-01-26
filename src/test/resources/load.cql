
CREATE KEYSPACE ${test.keyspace} WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE ${test.keyspace};

CREATE TABLE Resource (identifier text PRIMARY KEY, interactionModel text, quads text, parent text, modified timestamp);