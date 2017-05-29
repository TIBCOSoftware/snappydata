# Loading data from External Repositories

<mark>Jags comment</mark>
Like S3, CSV, HDFS, RDBMS and NoSQL
 (Should be a separate section if we have enough content ... )
- how to load from S3, CSV, HDFS using SQL external tables

	- How to deal with missing data or ignoring errors (CSV)

- How to manage parallelism for loading

- how to transform datatypes before loading (using SQL Cast, expressions)

- How to transform and enrich while loading (Using spark program)

- How to load an entire RDB (from a schema)? Example that reads all table names from catalog and then execute select from external JDBC source table on each table?

- How to load data from NoSQL stores like cassandra (give Cassandra example using Spark-cassandra package)