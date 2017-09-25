<a id="start_benchmark"></a>
# SnappyData Performance: 16x-20x faster than Apache Spark
Here you are walked through a simple benchmark to compare SnappyData's performance to Spark 2.1.1.
Millions of rows are loaded into a cached Spark DataFrame, run some analytic queries measuring its performance and then, repeat the same using SnappyData's column table. 

A simple analytic query that scans a 100 million-row column table shows SnappyData outperforming Apache Spark by 16-20X when both products have all the data in memory.

!!! Note: 
	It is recommended that you should have at least 4GB of RAM reserved for this test. 
 
## Start the Spark shell

Use any of the options mentioned below to start the Spark shell:

**If you are using your own Spark distribution that is compatible with version 2.1.1:**

    ```bash
    # Create a directory for SnappyData artifacts
    $ mkdir quickstartdatadir
    $ ./bin/spark-shell --driver-memory=4g --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --packages "SnappyDataInc:snappydata:1.0.0-s_2.11" --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
    ```

**If you have downloaded SnappyData**:

```bash
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$ ./bin/spark-shell --driver-memory=4g --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

**If you are using Docker**:
```bash
$ docker run -it -p 5050:5050 snappydatainc/snappydata bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

## To get the performance numbers
Ensure that you are in a Spark shell, and then follow the instructions below to get the performance numbers.

**Define a function "benchmark"**, which tells us the average time to run queries after doing the initial warm-ups.
```scala
scala>  def benchmark(name: String, times: Int = 10, warmups: Int = 6)(f: => Unit) {
          for (i <- 1 to warmups) {
            f
          }
          val startTime = System.nanoTime
          for (i <- 1 to times) {
            f
          }
          val endTime = System.nanoTime
          println(s"Average time taken in $name for $times runs: " +
            (endTime - startTime).toDouble / (times * 1000000.0) + " millis")
        }
```

**Create a DataFrame and temp table using Spark's range method**:
Cache it in Spark to get optimal performance. This creates a DataFrame of 100 million records.You can change the number of rows based on  your memory availability.
```scala
scala>  var testDF = spark.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as STRING)) as sym")
scala>  testDF.cache
scala>  testDF.createOrReplaceTempView("sparkCacheTable")
```

**Run a query and to check the performance**:
The queries use average of a field, without any where clause. This ensures that it touches all records while scanning.
```scala
scala>  benchmark("Spark perf") {spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()}
```

**Clean up the JVM**:
This ensures that all in memory artifacts for Spark is cleaned up.
```scala
scala>  testDF.unpersist()
scala>  System.gc()
scala>  System.runFinalization()
```

**Create a SnappyContext**:
```scala
scala>  val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
```

**Create similar 100 million record DataFrame**:
```scala
scala>  testDF = snappy.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar(10))) as sym")
```

**Create the table**:
```scala
scala>  snappy.sql("drop table if exists snappyTable")
scala>  snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")
```

**Insert the created DataFrame into the table and measure its performance**:
```scala
scala>  benchmark("Snappy insert perf", 1, 0) {testDF.write.insertInto("snappyTable") }
```

**Now let us run the same benchmark against Spark DataFrame**:
```scala
scala>  benchmark("Snappy perf") {snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()}
```

```
scala> :q // Quit the Spark Shell
```

!!! Note:
	This benchmark code is tested on a system with  4 CPUs (Intel(R) Core(TM) i7-5600U CPU @ 2.60GHz) and 16GiB System Memory. Also, in an AWS t2.xlarge (Variable ECUs, 4 vCPUs, 2.4 GHz, Intel Xeon Family, 16 GiB memory, EBS only) instance SnappyData is approximately 16 to 18 times faster than Spark 2.1.1.

