###Use your existing Spark installation
You can quickly check the functionality of SnappyData even with your existing Spark 2.0 installation. 
Only pre-requisites are you should have Spark Version 2.0 installed and 4GB of RAM.


```scala
$ cd $SPARK_HOME
$ ./bin/spark-shell --driver-memory 4g --packages "SnappyDataInc:snappydata:0.6.2-s_2.11"
```
This will open a Spark shell and download the relevant SnappyData files to your local machine.
Read and go through the code snippet one by one and copy them to Spark shell to execute.

* Start a SparkSesion in local mode
```scala
scala> import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder.master("local[4]").appName("spark, " +
                   "Snappy Quick Start").getOrCreate()
```

* Define a function name "elapsedTime" which will compute a method execution time
```scala
scala> def elapsedTime(name: String)(f: => Unit) {
           val startTime = System.nanoTime
           f
           val endTime = System.nanoTime
           println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
      }
```
* Create a DataFrame and temp table using Spark's range method. Cache it in Spark to get optimal performance.
  This will create a DataFrame of 100 million records.
```scala
scala> var df = spark.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
       df.cache
       df.createOrReplaceTempView("sparkCacheTable")
```

* Now run some query and to check the performance. The queries are mostly using average and group by.
This will ensure it touches all records while scanning.
```scala
scala> elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable").show}

scala> elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable").show}

scala> elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable").show}

scala> elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable group by (id%100)").show}
```
* Clean up the JVM
```scala
scala> df.unpersist()
scala> System.gc()
scala> System.runFinalization()
```

* Create a SnappySesion
```scala
scala> val snappy = org.apache.spark.sql.SnappyContext.apply().snappySession
```
* Create a similar 100 million record DataFrame
```scala
scala> df = snappy.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
```
* Create a column table in SnappyData. Also insert the created a DataFrame into the table

```scala
scala> df.write.format("column").saveAsTable("snappyColumnTable")
```
* Check the total row count now.
```scala
scala> snappy.sql("select * from snappyTable").count
```

* Run the same set of query and observe the performance difference between SparkSession and SnappySession
```scala
scala> elapsedTime("Snappy perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable").show}

scala> elapsedTime("Snappy perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable").show}

scala> elapsedTime("Snappy perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable").show}

scala> elapsedTime("Snappy perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable group by (id%100)").show}
```
* Unlike Spark DataFrames SnappyData column tables are mutable. You can insert rows to a column table.
The following code snippet create a Row object using Spark's API and inserts the Row to the table.

```scala
scala> import org.apache.spark.sql.Row
scala> snappy.insert("snappyColumnTable", Row.fromSeq(Seq(100000L, 1000000L)))
```
* Check the total row count now.
```scala
scala> snappy.sql("select * from snappyTable").count
```
* Snapppy also supports "Row" format tables. Row format tables are used for OLTP class of queries.
Below snippet uses a smaller DataFrame to create a Row table. We have defined a primary key for the row
table.

```scala
scala> df = snappy.range(1000).selectExpr("id", "floor(rand() * 10000) as k")

scala> snappy.sql("Create Table snappyRowTable(id bigint not null PRIMARY KEY, k bigint not null) USING row")

scala> df.write.insertInto("snappyRowTable")
scala> snappy.sql("select * from snappyRowTable").count
```

Let's mutate some rows on the table.

* Inserting new rows
```
scala> snappy.insert("snappyRowTable", Row.fromSeq(Seq(1001L, 1001L)))
scala> snappy.sql("select * from snappyRowTable").count
```
 * Upserting a row with ID 1001
```
scala> snappy.put("snappyRowTable", Row.fromSeq(Seq(1001L, 1002L)))
scala> snappy.sql("select * from snappyRowTable where ID = 1001").show
```

* Updating a row with ID 3
```
scala> snappy.update("snappyRowTable", "ID=3", Row.fromSeq(Seq(3L)), "K")

scala> snappy.sql("select * from snappyRowTable where ID = 3").show
```

* Quit the Spark shell

```
scala> :q
```
###Set up a simple cluster to use SnappyData#
Now that you have used SnappyData featueres from Spark shell in local cluster mode, we can move to a clustered setup on your local machine. We will
download the SnappyData binaries to your local machine and create some tables.

* Download link
* Start the SnappyData cluster. This will start a simple local cluster. The started components are a single data node, one lead node and one locator.

```scala
$ cd $SNAPPY_HOME
$ ./sbin/snappy-start-all.sh
```
   For detailed configuration you can refer <>. But to run this example the above setup suffice.

* Connect to SnappyData cluster by Snappy shell.
```scala
$ bin/snappy-shell
```
4. submit jobs
5. queries

