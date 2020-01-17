drop table if exists customerSnappy_staging;
CREATE external table customerSnappy_staging using org.apache.spark.sql.cassandra options (table 'customer', keyspace 'test',spark.cassandra.input.fetch.size_in_rows '200000', spark.cassandra.read.timeout_ms '10000');
drop table if exists customerSnappy;
CREATE table customerSnappy using column options() AS (SELECT * FROM customerSnappy_staging);