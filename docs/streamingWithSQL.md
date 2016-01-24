SnappyData’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and integration with the built-in store. 
Here is a brief overview of [Spark streaming](Spark streaming) from the Spark Streaming guide. 

## Spark Streaming Overview
Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Flume, Twitter, ZeroMQ, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like `map`, `reduce`, `join` and `window`.

Finally, processed data can be pushed out to filesystems, databases, and live dashboards. In fact, you can apply Spark's [machine learning](mllib-guide.html) and [graph processing](graphx-programming-guide.html) algorithms on data streams.  

<p style="text-align: center;">
  <img
    src="http://spark.apache.org/docs/latest/img/streaming-arch.png"
    title="Spark Streaming architecture"     alt="Spark Streaming"
    width="70%"
  />
</p>

Internally, it works as follows. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

<p style="text-align: center;">
 <img src="http://spark.apache.org/docs/latest/img/streaming-flow.png"
 title="Spark Streaming data flow"
 alt="Spark Streaming"
 width="70%" />
 </p>
 
 Spark Streaming provides a high-level abstraction called *discretized stream* or *DStream*, which represents a continuous stream of data. DStreams can be created either from input data streams from sources such as Kafka, Flume, and Kinesis, or by applying high-level operations on other DStreams. Internally, a DStream is represented as a sequence of [RDDs](api/scala/index.html#org.apache.spark.rdd.RDD). 

Additional details on the Spark Streaming concepts and programming is covered [here](Spark Streaming).

## SnappyData Streaming extensions over Spark
We offer the following enhancements over Spark Streaming : 

1. __Manage Streams declaratively__: Similar to SQL Tables, Streams can be defined declaratively from any SQL client and managed as Tables in the persistent system catalog of SnappyStore. The declarative language follows the SQL language and provides access to the any of the Spark Streaming streaming adapters such as Kafka or file input streams. Raw tuples arriving can be transformed into a proper structure through pluggable transformers providing the desired flexibility for custom filtering or type conversions. 

2. __SQL based stream processing__: With streams visible as Tables they can be joined with other streams or resident tables (reference data, history, etc). Essentially, the entire SQL language can be used to analyze distributed streams. 

3. __Continuous queries and time windows__: Similar to popular stream processing products, applications can register “continuous” queries on streams. By default, spark streaming emits batches once every second and any registered queries would be executed each time a batch is emitted. To support arbitrary time ranges, we extend the standard SQL to be able to specify the time window for the query. 

4. __OLAP optimizations__: By integrating and collocating stream processing with our hybrid in-memory storage engine, we leverage our optimizer and column store for expensive scans and aggregations, while providing fast key-based operations with our row store.

5. __Reduced shuffling through co-partitioning__: With SnappyData, the partitioning key used by the input queue (e.g., for Kafka sources), the stream processor and the underlying store can all be the same. This dramatically reduces the need to shuffle records.

6. __Approximate stream analytics__: When the volumes are too high, a stream can be summarized using various forms of samples and sketches to enable fast time series analytics. This is particularly useful when applications are interested in trending patterns, for instance, rendering a set of trend lines in real time on user displays.


## Working with stream tables

Describe SQL syntax for creating one.
Provide examples using the shell.
Life cycle commands to manage a streaming app
Running SQL queries dynamically from clients on stream tables

Provide examples when created using Snappy Context. Returned SchemaDStream is described next.


## SchemaDStream
How is it different than a DStream?
Describe its API in brief?
Provide some examples.

## Registering Continuous queries
How do you register on SchemaDStreams? 
How can you control the time windows for such queries?

## What is currently out-of-scope?

