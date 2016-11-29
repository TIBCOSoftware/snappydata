#### If you are an existing Spark users
You can quickly check the functionality of SnappyData even with your existing Spark 2.0 installation. 

Pre-requisites :
  Spark Version 2.0, 4GB of RAM


```scala
cd $SPARK_HOME
./bin/spark-shell --driver-memory 4g --packages "SnappyDataInc:snappydata:0.6.2-s_2.11"
```
 This will open a Spark shell and download the relevant SnappyData files to your local machine.
 Read and go through the code snippet one by one and copy them to Spark shell to execute.

Start a SparkSesion in local mode
```scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.master("local[4]").appName("spark, " +
  "Snappy Quick Start").getOrCreate()
```

Define a function name "benchmark" which will compute a method execution time
```scala
def elapsedTime(name: String)(f: => Unit) {
  val startTime = System.nanoTime
f
val endTime = System.nanoTime
println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}
```
Create a DataFrame using Spark's range method. This will create a DataFrame of 100 million records.
```scala
var df = spark.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
```
Cache it in Spark to get optimal performance. 
```scala
df.cache
```
Create a temporary table in Spark
```scala
df.createOrReplaceTempView("sparkCacheTable")
```
Now run some query and to check the performance
```scala
elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable").show}
elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable").show}
elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from sparkCacheTable").show}
elapsedTime("Spark perf") {spark.sql("select avg(k), avg(id) from " +
  "sparkCacheTable group by (id%100)").show}
```
Clean up the JVM
```scala
df.unpersist()
System.gc()
System.runFinalization()
```

Create a SnappySesion
```scala
val snappy = org.apache.spark.sql.SnappyContext.apply().snappySession
```
Create a similar 100 million record DataFrame
```scala
df = snappy.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
```
Create a column table in SnappyData. Also insert the created a DataFrame into the table

```scala
df.write.format("column").saveAsTable("snappyColumnTable")
```

Run the same set of query and observe the performance difference between SparkSession and SnappySession
```scala
elapsedTime("Spark perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable").show}
elapsedTime("Spark perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable").show}
elapsedTime("Spark perf") {snappy.sql("select avg(k), avg(id) from snappyColumnTable").show}
elapsedTime("Spark perf") {snappy.sql("select avg(k), avg(id) from " +
  "snappyColumnTable group by (id%100)").show}
```
Unlike Spark DataFrames SnappyData column tables are mutable. You can insert rows to a column table.

```scala
snappy.insert("snappyColumnTable", Row.fromSeq(Seq(100000L, 1000000L)))
```

Now we will create a Row format table with DataFrame

```scala
df.write.format("row").saveAsTable("snappyRowTable")
```


Quit the Spark shell

```
:q
```
#### If you want to use SQL with a simple cluster

#################Download link
#################snappy-startAll.sh
#################snappy-shell
#################submit jobs
#################queries

