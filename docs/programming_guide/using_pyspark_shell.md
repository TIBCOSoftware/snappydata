# Using pyspark Shell

By default pyspark shell does not support SnappySession. To  use SnappySession with pyspark shell, the shell must  be started with in-memory catalog implementation. 

Command to start pyspark shell with SnappySession:

```
./bin/pyspark --conf spark.sql.catalogImplementation=in-memory --conf spark.snappydata.connection=<locatorhost>:<clientPort>
```

**For example:**

```
./bin/pyspark --conf spark.sql.catalogImplementation=in-memory --conf spark.snappydata.connection=localhost:1527
```

!!!Note 
	When started with in-memory catalog implementation, hive support is not enabled.
