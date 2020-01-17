<a id="snappysession"></a>
# SparkSession, SnappySession and SnappyStreamingContext

## Create a SparkSession
[Spark Context](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/SparkContext.html) is the main entry point for Spark functionality. A SparkContext represents the connection to a Spark cluster and can be used to create RDDs, accumulators and broadcast variables on that cluster.

[Spark Session](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/SparkSession.html) is the entry point to programming Spark with the Dataset and DataFrame API.
SparkSession object can be created by using SparkSession.Builder used as below.

```pre
SparkSession.builder()
     .master("local")
     .appName("Word Count")
     .config("spark.some.config.option", "some-value")
     .getOrCreate()
```

In environments where SparkSession has been created up front (e.g. REPL, notebooks), use the builder to get an existing session:

```pre
SparkSession.builder().getOrCreate()
```

## Create a SnappySession
[SnappySession](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappySession) is the main entry point for SnappyData extensions to Spark. A SnappySession extends Spark's [SparkSession](http://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.sql.SparkSession) to work with Row and Column tables. Any DataFrame can be managed as a SnappyData table and any table can be accessed as a DataFrame.

To create a SnappySession:

**Scala**

```pre
 val spark: SparkSession = SparkSession
         .builder
         .appName("SparkApp")
         .master("master_url")
         .getOrCreate
        
 val snappy = new SnappySession(spark.sparkContext)
```
**Java**

```pre
 SparkSession spark = SparkSession
       .builder()
       .appName("SparkApp")
       .master("master_url")
       .getOrCreate();
      
 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
 SnappySession snappy = new SnappySession(spark.sparkContext());
```

**Python**

```pre
 from pyspark.sql.snappy import SnappySession
 from pyspark import SparkContext, SparkConf
 
 conf = SparkConf().setAppName(appName).setMaster(master)
 sc = SparkContext(conf=conf)
 snappy = SnappySession(sc)
```

!!!Note
	Dataframe created from Snappy session and Spark session cannot be used across interchangeably.

## Create a SnappyStreamingContext
[SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.streaming.SnappyStreamingContext) is an entry point for SnappyData extensions to Spark Streaming and it extends Spark's
[Streaming Context](http://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.streaming.StreamingContext).

To create a SnappyStreamingContext:

**Scala**

```pre
 val spark: SparkSession = SparkSession
         .builder
         .appName("SparkApp")
         .master("master_url")
         .getOrCreate
 val snsc = new SnappyStreamingContext(spark.sparkContext, Duration(1))
```
**Java**

```pre
 SparkSession spark = SparkSession
     .builder()
     .appName("SparkApp")
     .master("master_url")
     .getOrCreate();

 JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

 Duration batchDuration = Milliseconds.apply(500);
 JavaSnappyStreamingContext jsnsc = new JavaSnappyStreamingContext(jsc, batchDuration);
```

**Python**

```pre
 from pyspark.streaming.snappy.context import SnappyStreamingContext
 from pyspark import SparkContext, SparkConf
 
 conf = SparkConf().setAppName(appName).setMaster(master)
 sc = SparkContext(conf=conf)
 duration = .5
 snsc = SnappyStreamingContext(sc, duration)
```

Also, SnappyData can be run in three different modes, Local Mode, Embedded Mode and SnappyData Connector mode. Before proceeding, it is important that you understand these modes. For more information, see [Affinity modes](../deployment.md).

If you are using SnappyData in LocalMode or Connector mode, it is the responsibility of the user to create a SnappySession.
If you are in the Embedded Mode, applications typically submit jobs to SnappyData and do not explicitly create a SnappySession or SnappyStreamingContext.
Jobs are the primary mechanism to interact with SnappyData using the Spark API in embedded mode.
A job implements either SnappySQLJob or SnappyStreamingJob (for streaming applications) trait.

