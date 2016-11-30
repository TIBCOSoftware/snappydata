###Use your existing Spark installation
You can quickly check the functionality of SnappyData even with your existing Spark 2.0 installation. 
Only pre-requisites are you should have Spark Version 2.0 installed. Preferably you should have at least 4GB of RAM for the application.



```scala
$ cd <Spark_Install_dir>
$ ./bin/spark-shell --driver-memory 4g --packages "SnappyDataInc:snappydata:0.6.2-s_2.11"
```
This will open a Spark shell and download the relevant SnappyData files to your local machine.
Read and go through the code snippet one by one and copy them to Spark shell to execute.

* Start a SparkSesion in local mode
```scala
scala> import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder.master("local[*]").appName("spark, " +
                   "Snappy Quick Start").getOrCreate()
```

* Define a of helper function "benchmark", which will give us an average time of a query after doing initial warmups.
```scala
scala>  def benchmark(name: String, times: Int = 10, warmups: Int = 2)(f: => Unit) {
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
* Create a DataFrame and temp table using Spark's range method. Cache it in Spark to get optimal performance.
  This will create a DataFrame of 100 million records.
```scala
scala> var df = spark.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
scala> df.cache
scala> df.createOrReplaceTempView("sparkCacheTable")
```

* Now run a query and to check the performance. The queries is using average of a field without any where clause.
This will ensure it touches all records while scanning.
```scala
scala> benchmark("Spark perf") { spark.sql("select avg(k) from sparkCacheTable").collect() }

scala> spark.sql("select avg(k) from sparkCacheTable").show()

scala> benchmark("Spark perf") {spark.sql("select avg(k) from sparkCacheTable group by (id%100)").collect}

scala> spark.sql("select avg(k) from sparkCacheTable group by (id%100)").show

```
* Clean up the JVM. This will ensure all in memory artifacts for Spark is cleaned up.
```scala
scala> df.unpersist()
scala> System.gc()
scala> System.runFinalization()
```

* Create a SnappyContext. A SnappyContext extends Spark's [[org.apache.spark.sql.SQLContext]] to work with Row and Columnar tables.
  Any DataFrame can be managed as SnappyData tables and any table can be accessed as a DataFrame. This integrates the SQLContext
  functionality with the Snappy store.
```scala
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext, existingSharedState = None)
```
* Create a similar 100 million record DataFrame
```scala
scala> df = snappy.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
```
* Create a columnar table in SnappyData. Also insert the created a DataFrame into the table

```scala
scala> snappy.sql("create table snappyColumnTable (id bigint not null, k bigint not null) using column")
scala> df.write.insertInto("snappyColumnTable")
```
* Check the total row count now.
```scala
scala> snappy.sql("select count(*) from snappyColumnTable").show
```

* Unlike Spark DataFrames SnappyData column tables are mutable. You can insert rows to a column table.
The following code snippet create a Row object using Spark's API and inserts the Row to the table.

```scala
scala> import org.apache.spark.sql.Row
scala> snappy.insert("snappyColumnTable", Row(100000L, 1000000L))
```
* Check the total row count now.
```scala
scala> snappy.sql("select count(*) from snappyColumnTable").show
```

* Now lets run the same benchmark we ran against Spark DataFrame.
```scala
scala> benchmark("Snappy perf") {snappy.sql("select avg(k) from snappyColumnTable").collect}

scala> snappy.sql("select avg(k) from snappyColumnTable").show

scala> benchmark("Snappy perf") {snappy.sql("select avg(k) from snappyColumnTable group by (id%100)").collect}

scala> snappy.sql("select avg(k) from snappyColumnTable group by (id%100)").show

```

* Snapppy also supports "Row" format tables. Row format tables are used for OLTP class of queries.
 First we will create a row table which has a primary key.

```scala
scala> snappy.sql("Create Table snappyRowTable(id bigint not null PRIMARY KEY, k bigint not null) USING row")
```
* Now let's insert 100K rows in the row table.
```
scala> df = snappy.range(100000).selectExpr("id", "floor(rand() * 10000) as k")
scala> df.write.insertInto("snappyRowTable")
scala> snappy.sql("select count(*) from snappyRowTable").show
```

Let's now some example APIs how to mutate a row table.

* Inserting new rows
```
scala> snappy.insert("snappyRowTable", Row(100001L, 100001L))
scala> snappy.sql("select count(*) from snappyRowTable").show
```
 * Upserting a row with ID 100001
```
scala> snappy.put("snappyRowTable", Row(100001L, 100002L))
scala> snappy.sql("select * from snappyRowTable where ID = 100001").show
```

* Updating a row with ID 100001. Update the new "K" to be 100003
```
scala> snappy.update("snappyRowTable", "ID=100001", Row(100003L), "K")

scala> snappy.sql("select * from snappyRowTable where ID = 100001").show
```

* Quit the Spark shell

```
scala> :q
```
###Set up a simple cluster to use SnappyData#
Now that you have used SnappyData features from Spark shell in local cluster mode, we can move to a clustered setup on your local machine. We will
download the SnappyData binaries to your local machine and create some tables.

* Download link
* Start the SnappyData cluster. This will start a simple local cluster. The started components are a single data node, one lead node and one locator.

```scala
$ cd <Snappy_Install_dir>
$ ./sbin/snappy-start-all.sh
```
   For detailed configuration you can refer <>. But to run this example the above setup suffice.

* Connect to SnappyData cluster by Snappy shell.
```scala
$ bin/snappy-shell
snappy> connect client 'localhost:1527';
snappy> create table snappyTable (id bigint not null, k bigint not null) using column;
```
4. submit jobs
5. queries

