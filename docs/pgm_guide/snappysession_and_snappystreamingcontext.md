<a id="snappysession"></a>
## SnappySession and SnappyStreamingContext

[SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession) is the main entry point for SnappyData extensions to Spark. A SnappySession extends Spark's [SparkSession](http://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.SparkSession) to work with Row and Column tables. Any DataFrame can be managed as a SnappyData table and any table can be accessed as a DataFrame.
Similarly, [SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.streaming.SnappyStreamingContext) is an entry point for SnappyData extensions to Spark Streaming and it extends Spark's
[Streaming Context](http://spark.apache.org/docs/2.0.0/api/scala/index.html#org.apache.spark.streaming.StreamingContext).

Also SnappyData can be run in three different modes, Local Mode, Embedded Mode and SnappyData Connector mode. Before proceeding, it is important that you understand these modes. For more information, see [SnappyData Spark Affinity modes](../deployment.md).

If you are using SnappyData in LocalMode or Connector mode, it is the responsibility of the user to create a SnappySession.

###To Create a SnappySession

#### Scala 

```scala
 val spark: SparkSession = SparkSession
         .builder
         .appName("SparkApp")
         .master("master_url")
         .getOrCreate
        
 val snappy = new SnappySession(spark.sparkContext)
```
#### Java

```Java
 SparkSession spark = SparkSession
       .builder()
       .appName("SparkApp")
       .master("master_url")
       .getOrCreate();
      
 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
 SnappySession snappy = new SnappySession(spark.sparkContext());
```

#### Python

```Python
 from pyspark.sql.snappy import SnappySession
 from pyspark import SparkContext, SparkConf
 
 conf = SparkConf().setAppName(appName).setMaster(master)
 sc = SparkContext(conf=conf)
 snappy = SnappySession(sc)
```

### To Create a SnappyStreamingContext
#### Scala

```scala
 val spark: SparkSession = SparkSession
         .builder
         .appName("SparkApp")
         .master("master_url")
         .getOrCreate
 val snsc = new SnappyStreamingContext(spark.sparkContext, Duration(1))
```
#### Java

```Java
 SparkSession spark = SparkSession
     .builder()
     .appName("SparkApp")
     .master("master_url")
     .getOrCreate();

 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

 Duration batchDuration = Milliseconds.apply(500);
 JavaSnappyStreamingContext jsnsc = new JavaSnappyStreamingContext(jsc, batchDuration);
```

#### Python

```Python
 from pyspark.streaming.snappy.context import SnappyStreamingContext
 from pyspark import SparkContext, SparkConf
 
 conf = SparkConf().setAppName(appName).setMaster(master)
 sc = SparkContext(conf=conf)
 duration = .5
 snsc = SnappyStreamingContext(sc, duration)
```

If you are in the Embedded Mode, applications typically submit Jobs to SnappyData and do not explicitly create a SnappySession or SnappyStreamingContext. 
These jobs are the primary mechanism to interact with SnappyData using the Spark API. 
A job implements either SnappySQLJob or SnappyStreamingJob (for streaming applications) trait.

The implementation of the _runSnappyJob_ function from SnappySQLJob uses a SnappySession to interact with the SnappyData store to process and store tables.
The implementation of _runSnappyJob_ from SnappyStreamingJob uses a SnappyStreamingContext to create streams and manage the streaming context.
The jobs are submitted to the lead node of SnappyData over REST API using a _spark-submit_ like utility.
