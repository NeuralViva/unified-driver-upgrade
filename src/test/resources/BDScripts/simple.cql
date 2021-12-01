CREATE KEYSPACE inventory
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE inventory.product(id uuid PRIMARY KEY, description text);

INSERT INTO inventory.product(id, description) values(c37d661d-7e61-49ea-96a5-68c34e83db3a,'Laptop');