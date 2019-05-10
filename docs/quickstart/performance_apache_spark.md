<a id="start_benchmark"></a>
# Benchmark 16-20x Faster Performance than Apache Spark
In this section, you are walked through a simple benchmark to compare TIBCO ComputeDB's performance to Spark 2.1.1.</br>
Millions of rows are loaded into a cached Spark DataFrame, some analytic queries measuring its performance are run, and then, the same using TIBCO ComputeDB's column table is repeated.

A simple analytic query that scans a 100 million-row column table shows TIBCO ComputeDB outperforming Apache Spark by 16-20X when both products have all the data in memory.

!!! Note
	It is recommended that you should have at least 4GB of RAM reserved for this test. 
 
## Start the Spark Shell

Use any of the options mentioned below to start the Spark shell:

* **If you are using your own Spark distribution that is compatible with version 2.1.1:**

        # Create a directory for TIBCO ComputeDB artifacts
        $ mkdir quickstartdatadir 
        $ ./bin/spark-shell --driver-memory=4g --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --packages "SnappyDataInc:snappydata:1.1.0-s_2.11" --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"

* **If you have downloaded TIBCO ComputeDB**:

        # Create a directory for TIBCO ComputeDB artifacts
        $ mkdir quickstartdatadir 
        $ ./bin/spark-shell --driver-memory=4g --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"

* **If you are using Docker**:

        $ docker run -it -p 5050:5050 snappydatainc/snappydata bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"

## To get the Performance Numbers
Ensure that you are in a Spark shell, and then follow the instructions below to get the performance numbers.

1. **Define a function "benchmark"**, which tells us the average time to run queries after doing the initial warm-ups.

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

2. **Create a DataFrame and temporary table using Spark's range method**:

	Cache it in Spark to get optimal performance. This creates a DataFrame of 100 million records.You can change the number of rows, based on your memory availability.

        scala>  var testDF = spark.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as STRING)) as sym")
        scala>  testDF.cache
        scala>  testDF.createOrReplaceTempView("sparkCacheTable")

3. **Run a query and to check the performance**:

	The queries use an average of a field, without any "where" clause. This ensures that it touches all records while scanning.
		
        scala>  benchmark("Spark perf") {spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()}

4. **Clean up the JVM**:

	This ensures that all in-memory artifacts for Spark are cleaned up.

        scala>  testDF.unpersist()
        scala>  System.gc()
        scala>  System.runFinalization()

5. **Create a SnappySession**:

		scala>  val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)

6. **Create similar 100 million record DataFrame**:

		scala>  testDF = snappy.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar(10))) as sym")


7. **Create the table**:

        scala>  snappy.sql("drop table if exists snappyTable")
        scala>  snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")


8. **Insert the created DataFrame into the table and measure its performance**:

	    scala>  benchmark("Snappy insert perf", 1, 0) {testDF.write.insertInto("snappyTable") }

9. **Now, let us run the same benchmark against Spark DataFrame**:

        scala>  benchmark("Snappy perf") {snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()}


	    scala> :q // Quit the Spark Shell

!!! Note
	This benchmark code is tested on a system with  4 CPUs (Intel(R) Core(TM) i7-5600U CPU @ 2.60GHz) and 16GiB System Memory. Also, in an AWS t2.xlarge (Variable ECUs, 4 vCPUs, 2.4 GHz, Intel Xeon Family, 16 GiB memory, EBS only) instance TIBCO ComputeDB is approximately 16 to 18 times faster than Spark 2.1.1.
