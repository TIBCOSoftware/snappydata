<a id="Start Benchmark"></a>
##Option 3: 20X Faster than Spark 2.0.2
Here we walk you through a simple benchmark to compare SnappyData to Spark 2.0.2 performance.
We load millions of rows into a cached Spark DataFrame, run some analytic queries measuring its performance and then, repeat the same using SnappyData's column table. 

!!! Note
	It is recommended that you should have at least 4GB of RAM reserved for this test. </Note>
 

**Start the Spark Shell using any of the options mentioned below:**

**If you are using your own Spark 2.0.2 installation:**

```bash
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$ ./bin/spark-shell --driver-memory=4g --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --packages "SnappyDataInc:snappydata:0.7-s_2.11" --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

**If you have downloaded SnappyData **:

```bash
# Create a directory for SnappyData artifacts
$ mkdir quickstartdatadir
$ ./bin/spark-shell --driver-memory=4g --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

** If you are using Docker**:
```bash
$ docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

### To get the Performance Numbers
Ensure that you are in a Spark Shell, and then follow the instruction below to get the performance numbers.

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

**Create a SnappyContex**:
```scala
scala>  val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
```

** Create similar 100 million record DataFrame**:
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

<Note>  We have tested this benchmark code in system with  4 CPUs (Intel(R) Core(TM) i7-5600U CPU @ 2.60GHz) and 16GiB System Memory. In a AWS
t2.xlarge (Variable ECUs, 4 vCPUs, 2.4 GHz, Intel Xeon Family, 16 GiB memory, EBS only) instance too SnappyData is approx 16 to 18 times fatser than Spark 2.0.2 .
</Note>