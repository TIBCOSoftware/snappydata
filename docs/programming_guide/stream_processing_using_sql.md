# Stream Processing using SQL
TIBCO ComputeDB’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and integration with the built-in store. 
Here is a brief overview of [Spark streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html) from the Spark Streaming guide. 


## Spark Streaming Overview

Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Flume, Twitter, ZeroMQ, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like **map**, **reduce**, **join** and **window**.

Finally, processed data can be pushed out to filesystems, databases, and live dashboards. In fact, you can apply Spark's [machine learning](http://spark.apache.org/docs/latest/mllib-guide.html) and [graph processing](http://spark.apache.org/docs/latest/graphx-programming-guide.html) algorithms on data streams.

![Spark Streaming architecture](http://spark.apache.org/docs/latest/img/streaming-arch.png)

Internally, it works as follows. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

![Spark Streaming data flow](http://spark.apache.org/docs/latest/img/streaming-flow.png)
 
 Spark Streaming provides a high-level abstraction called *discretized stream* or *DStream*, which represents a continuous stream of data. DStreams can be created either from input data streams from sources such as Kafka, Flume, and Kinesis, or by applying high-level operations on other DStreams. Internally, a DStream is represented as a sequence of [RDDs](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/rdd/RDD.html). 

Additional details on the Spark Streaming concepts and programming is covered [here](http://spark.apache.org/docs/latest/streaming-programming-guide.html).

## TIBCO ComputeDB Streaming Extensions over Spark
The following enhancements over Spark Streaming are provided: 

1. __Manage Streams declaratively__: Similar to SQL Tables, Streams can be defined declaratively from any SQL client and managed as Tables in the persistent system catalog of SnappyStore. The declarative language follows the SQL language and provides access to any of the Spark Streaming streaming adapters such as Kafka or file input streams. Raw tuples arriving can be transformed into a proper structure through pluggable transformers providing the desired flexibility for custom filtering or type conversions. 

2. __SQL based stream processing__: With streams visible as Tables they can be joined with other streams or resident tables (reference data, history, etc). Essentially, the entire SQL language can be used to analyze distributed streams. 

3. __Continuous queries and time windows__: Similar to popular stream processing products, applications can register “continuous” queries on streams. By default, Spark streaming emits batches once every second and any registered queries would be executed each time a batch is emitted. To support arbitrary time ranges, the standard SQL is extended to be able to specify the time window for the query. 

4. __OLAP optimizations__: By integrating and colocating stream processing with the hybrid in-memory storage engine, the product leverages the optimizer and column store for expensive scans and aggregations, while providing fast key-based operations with RowStore.

5. __Approximate stream analytics__: When the volumes are too high, a stream can be summarized using various forms of samples and sketches to enable fast time series analytics. This is particularly useful when applications are interested in trending patterns, for instance, rendering a set of trend lines in real time on user displays.

## Working with Stream Tables
TIBCO ComputeDB supports creation of stream tables from Twitter, Kafka, Files, Sockets sources.

```pre
// DDL for creating a stream table
CREATE STREAM TABLE [IF NOT EXISTS] table_name
(COLUMN_DEFINITION)
USING 'kafka_stream | file_stream | twitter_stream | socket_stream'
OPTIONS (
// multiple stream source specific options
  storagelevel '',
  rowConverter '',
  subscribe '',
  kafkaParams '',
  consumerKey '',
  consumerSecret '',
  accessToken '',
  accessTokenSecret '',
  hostname '',
  port '',
  directory ''
)

// DDL for dropping a stream table
DROP TABLE [IF EXISTS] table_name

// Initialize StreamingContext
STREAMING INIT <batchInterval> [SECS|SECOND|MILLIS|MILLISECOND|MINS|MINUTE]

// Start streaming
STREAMING START

// Stop streaming
STREAMING STOP
```

For example to create a stream table using kafka source : 
```pre
 val spark: SparkSession = SparkSession
     .builder
     .appName("SparkApp")
     .master("local[4]")
     .getOrCreate

 val snsc = new SnappyStreamingContext(spark.sparkContext, Duration(1))

 snsc.sql("create stream table streamTable (userId string, clickStreamLog string) " +
     "using kafka_stream options (" +
     "storagelevel 'MEMORY_AND_DISK_SER_2', " +
     "rowConverter 'io.snappydata.app.streaming.KafkaStreamToRowsConverter', " +
     "kafkaParams 'zookeeper.connect->localhost:2181;auto.offset.reset->smallest;group.id->myGroupId', " +
     "subscribe 'streamTopic:01')")

 // You can get a handle of underlying DStream of the table
 val dStream = snsc.getSchemaDStream("streamTable")

 // You can also save the DataFrames to an external table
 dStream.foreachDataFrame(_.write.insertInto(tableName))
```
The streamTable created in the above example can be accessed from snappy-sql and can be queried using ad-hoc SQL queries.

## Stream SQL through snappy-sql
Start a TIBCO ComputeDB cluster and connect through snappy-sql :

```pre
//create a connection
snappy> connect client 'localhost:1527';

// Initialize streaming with batchInterval of 2 seconds
snappy> streaming init 2secs;

// Create a stream table
snappy> create stream table streamTable (id long, text string, fullName string, country string,
        retweets int, hashtag  string) using twitter_stream options (consumerKey '', consumerSecret '',
        accessToken '', accessTokenSecret '', rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter');

// Start the streaming
snappy> streaming start;

//Run ad-hoc queries on the streamTable on current batch of data
snappy> select id, text, fullName from streamTable where text like '%snappy%'

// Drop the streamTable
snappy> drop table streamTable;

// Stop the streaming
snappy> streaming stop;
```

## SchemaDStream
SchemaDStream is SQL based DStream with support for schema/Product. It offers the ability to manipulate SQL queries on DStreams. It is similar to SchemaRDD, which offers similar functions. Internally, RDD of each batch duration is treated as a small table and CQs are evaluated on those small tables. Similar to foreachRDD in DStream, SchemaDStream provides foreachDataFrame API. SchemaDStream can be registered as a table.
Some of these ideas (especially naming our abstractions) were borrowed from [Intel's Streaming SQL project](https://github.com/Intel-bigdata/spark-streamingsql).

## Registering Continuous Queries
```pre
//You can join two stream tables and produce a result stream.
val resultStream = snsc.registerCQ("SELECT s1.id, s1.text FROM stream1 window (duration
    '2' seconds, slide '2' seconds) s1 JOIN stream2 s2 ON s1.id = s2.id")
    
// You can also save the DataFrames to an external table
dStream.foreachDataFrame(_.write.insertInto("yourTableName"))
```

## Dynamic (ad-hoc) Continuous Queries
Unlike Spark streaming, you do not need to register all your stream output transformations (which is a continuous query in this case) before the start of StreamingContext. The continuous queries can be registered even after the [SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.streaming.SnappyStreamingContext) has started.

