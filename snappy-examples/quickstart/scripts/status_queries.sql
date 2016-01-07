---- Members in the Snappy Distributed System ----
SHOW MEMBERS;

---- Tables in the DB are ----
SHOW TABLES in APP;

---- Schema for column table ----
DESCRIBE airline;

---- Column table has the following number of entries ----
SELECT COUNT(*) as TotalColTableEntries
  FROM airline;

---- Schema for Row table ----
DESCRIBE airlineref;

---- Row table has the following number of entries ----
SELECT COUNT(*) AS TotalRowTableEntries
  FROM airlineref;

