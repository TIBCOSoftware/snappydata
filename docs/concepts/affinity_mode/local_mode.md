<a id="localmode"></a>
##Local Mode

In this mode, you can execute all the components (client application, executors, and data store) locally in the application's JVM. It is the simplest way to start testing and using SnappyData, as you do not require a cluster, and the  executor threads are launched locally for processing.

**Key Points**

* No cluster required

* Launch Single JVM (Single-node Cluster)

* Launches executor threads locally for processing

* Embeds the SnappyData in-memory store in-process

* For development purposes only
 
![Local Mode](../Images/SnappyLocalMode.png)

**Example**: **Using the Local mode for developing SnappyData programs**

You can use an IDE of your choice, and provide the below dependency to get SnappyData binaries:

**Example: Maven dependency**
```
<!-- https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11 -->
<dependency>
    <groupId>io.snappydata</groupId>
    <artifactId>snappydata-cluster_2.11</artifactId>
    <version>0.7</version>
</dependency>
```
**Example: SBT dependency**

```
// https://mvnrepository.com/artifact/io.snappydata/snappydata-cluster_2.11
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "0.7"

```
**Create SnappySession**: To start SnappyData store you need to create a SnappySession in your program
```scala
 val spark: SparkSession = SparkSession
         .builder
         .appName("SparkApp")
         .master("local[*]")
         .getOrCreate
 val snappy = new SnappySession(spark.sparkContext)
```
  
  
  
**Example**: **Launch Apache Spark shell and provide SnappyData dependency as a Spark package**:
If you already have Spark2.0 installed in your local machine you can directly use `--packages` option to download the SnappyData binaries.
```bash
./bin/spark-shell --packages "SnappyDataInc:snappydata:0.7-s_2.11"
```
