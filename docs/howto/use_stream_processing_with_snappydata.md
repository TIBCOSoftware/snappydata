<a id="howto-streams"></a>

# How to use Stream Processing with SnappyData 

SnappyData supports both the older [Spark Streaming model (based on DStreams)](#dstreams) as well as the newer [Structured Streaming model](#structuredstreaming). Unlike the Spark streaming DStreams model, that is based on RDDs, SnappyData supports Spark SQL in both models.

<a id= dstreams> </a>
## Spark Streaming DStreams Model

SnappyData’s streaming functionality builds on top of Spark Streaming and is primarily aimed at making it simpler to build streaming applications and to integrate with the built-in store. In SnappyData, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate SnappyData tables based on the streaming data. For more information on streaming, refer to this [section](../programming_guide/stream_processing_using_sql.md).

### Code Sample

Code example for streaming is in [StreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StreamingExample.scala). The code snippets in the following sections show how to declare a stream table, register continuous queries(CQ), and update SnappyData table using the stream data.

### Using Stream Processing with SnappyData

**First get a SnappySession and a SnappyStreamingContext**: </br>
Here SnappyStreamingContext is initialized in a batch duration of one second.
```pre
val spark: SparkSession = SparkSession
    .builder
    .appName(getClass.getSimpleName)
    .master("local[*]")
    .getOrCreate

val snsc = new SnappyStreamingContext(spark.sparkContext, Seconds(1))
```

The example starts an embedded Kafka instance on which a few messages are published. SnappyData processes these message and updates a table based on the stream data.

The SQL below shows how to declare a stream table using SQL. The rowConverter attribute specifies a class used to return Row objects from the received stream messages.
```pre
 snsc.sql(
      "create stream table adImpressionStream (" +
        " time_stamp timestamp," +
        " publisher string," +
        " advertiser string," +
        " website string," +
        " geo string," +
        " bid double," +
        " cookie string) " + " using kafka_stream options(" +
        " rowConverter 'org.apache.spark.examples.snappydata.RowsConverter'," +
        s" kafkaParams 'bootstrap.servers->$add;" +
        "key.deserializer->org.apache.kafka.common.serialization.StringDeserializer;" +
        "value.deserializer->org.apache.kafka.common.serialization.StringDeserializer;" +
        s"group.id->$groupId;auto.offset.reset->earliest'," +
        s" startingOffsets '$startingOffsets', " +
        s" subscribe '$topic')"
    )
```

RowsConverter decodes a stream message consisting of comma-separated fields and forms a Row object from it.

```pre
class RowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val log = message.asInstanceOf[String]
    val fields = log.split(",")
    val rows = Seq(Row.fromSeq(Seq(new java.sql.Timestamp(fields(0).toLong),
      fields(1),
      fields(2),
      fields(3),
      fields(4),
      fields(5).toDouble,
      fields(6)
    )))

    rows
  }
}

```

**To create a row table that is updated based on the streaming data**:

```pre
snsc.sql("create table publisher_bid_counts(publisher string, bidCount int) using row")
```

**To declare a continuous query that is executed on the streaming data**: This query returns a number of bids per publisher in one batch.

```pre
val resultStream: SchemaDStream = snsc.registerCQ("select publisher, count(bid) as bidCount from " +
    "adImpressionStream window (duration 1 seconds, slide 1 seconds) group by publisher")
```

**To process that the result of above continuous query to update the row table publisher_bid_counts**:

```pre
// this conf is used to get a JDBC connection
val conf = new ConnectionConfBuilder(snsc.snappySession).build()

resultStream.foreachDataFrame(df => {
    println("Data received in streaming window")
    df.show()

    println("Updating table publisher_bid_counts")
    val conn = ConnectionUtil.getConnection(conf)
    val result = df.collect()
    val stmt = conn.prepareStatement("update publisher_bid_counts set " +
        s"bidCount = bidCount + ? where publisher = ?")

    result.foreach(row => {
      val publisher = row.getString(0)
      val bidCount = row.getLong(1)
      stmt.setLong(1, bidCount)
      stmt.setString(2, publisher)
      stmt.addBatch()
        }
        )
    stmt.executeBatch()
    conn.close()
  }
})
```

**To display the total bids by each publisher by querying publisher_bid_counts table**:

```pre
snsc.snappySession.sql("select publisher, bidCount from publisher_bid_counts").show()
```
<a id= structuredstreaming> </a>
## Structured Streaming

The SnappyData structured streaming programming model is the same as [Spark structured streaming](https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html). 

The only difference is support for ingesting streaming dataframes into SnappyData tables through a built-in **Sink**. The **Sink** supports idempotent writes, ensuring consistency of data when failures occur, as well as support for all mutation operations such as inserts, appends, updates, puts, and deletes. 

The output data source name for SnappyData is `snappysink`.

### Code Sample

A minimal code example for structured streaming with snappysink is available [here](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingWithSnappySink.scala).

### Using SnappyData Structured Streaming API

The following code snippet, from the example, explains the usage of SnappyData's ** Structured Streaming API**:

```pre
    val streamingQuery = structDF
        .filter(_.signal > 10)
        .writeStream
        .format("snappysink")    		// Required to ingest into SnappyData tables
        .queryName("Devices")
        .trigger(ProcessingTime("1 seconds"))
        .option("streamQueryId", "Devices")     // Required: must be unique across a snappydata cluster
        .option("tableName", "devices")		// Required: where should the data be saved ? 
        .option("checkpointLocation", checkpointDirectory)
        .start()
```
#### SnappyData Specific options

The following are SnappyData specific options which can be configured for Structured Streaming:

| Options | Description |
|--------|--------|
|    `streamQueryId`    | This is internally used by SnappyData to track the progress of a stream query. The value of this property must be kept unique for each stream query across the SnappyData cluster.  The property is case-insensitive and is mandatory.|
|`tableName`|Name of the SnappyData table where the streaming data is ingested. The property is case-insensitive and is mandatory.|
|`conflation`|This is an optional boolean property with the default value set to `false`. Conflation is enabled only when you set this property to `true`. </br>If this property is set to `true` and if the incoming streaming batch contains multiple events on the same key, **SnappyData Sink** automatically reduces this to a single operation. This is typically the last operation on any given key for the batch that is being processed. This property is only applicable when the `_eventType` column is available (see [below](#eventypecolumn)) and the target table has Keys defined. For more information, see [here](#conflationpro). |
|<a id= snappycallback> </a>`sinkCallback`|This is an optional property which is used to override default **SnappyData Sink** behavior. To override the default behavior, client codes should implement `SnappySinkCallback` trait and pass the fully qualified name of the implementing class against this property value.|

### Handling Inserts, Updates and Deletes

A common use case for streaming is capturing writes into another store (Operational database such as RDB or NoSQL DB) and streaming the events through Kafka, applying Spark transformations, and ingesting into an analytics datastore such as SnappyData. This pattern is commonly referred to as **Change-Data-Capture (CDC)**.

<a id= eventypecolumn> </a>
To support this use case, **SnappyData Sink** supports events to signal if these are Inserts, Updates, or Deletes. The application is required to inject a column called `_eventType` as described below. 

To support **CDC**, the source DataFrame must have the following:

*	An `IntegerType` column named `_eventType`.  The value in the `_eventType` column can be any of the following:
  	-	`0` for insert events
	-	`1` for update events
	-	`2` for delete events

	In case the input data is following a different convention for event types, then it must be transformed to match the above-mentioned format.

    !!!Note
    	Records which have `_eventType` value other than the above-mentioned ones are skipped.

*	The target SnappyData table must have key columns defined for a column table or primary key defined for a row table.

An example explaining the **CDC** use case is available [here](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingCDCExample.scala).


If the `_eventType` column is not provided as part of source dataframe, then the following is observed:</br> 

-	In a target table with key columns/primary key defined, the **put into** operation is applied to all events.
-	In a target table without key columns/primary key defined, the **insert** operation is applied to all the events.

<a id= event_order> </a>
### Event Processing Order

Currently, the ordering of events across partitions is NOT supported. Event processing occurs independently in each partition. Hence, your application must ensure that all the events, that are associated with a key, are always delivered on the same partition (shard on the key).</br>If your incoming stream is not partitioned on the key column(s), the application should first repartition the dataframe on the key column(s). You can ignore this requirement, if your incoming streams are continuously appending (For example, time series) or when replacing data where ordering is irrelevant.

<a id= conflationpro> </a>
If `conflation` property is set to `true`, **SnappyData Sink** will first conflate the incoming batch independently by each partition. This results in a batch, where there is at most a single entry per key. </br>Then, the writes occur by grouping the events in the following manner (for performance optimization of the columnar store) and is only applicable when your DataFrame has an `_eventType` column:
     
*	**Processes all delete events** (deletes relevant records from target table).
*	**Processes all insert events** (inserts relevant records into the target table).
*	**Processes all update events **(applies **PutInto** operation).
	
This above grouping semantics is followed even when the `conflation` property is set to `false`.  When `_eventType` column is not available, all records are merged into the target table using **PutInto** semantics.

By default the `conflation` property is set to `false`. Therefore, the current ordering semantics only ensures consistency when incoming events in a batch are for unique key column(s).

**For example:**</br>If an incoming batch contains an **Insert(key1)** event followed by a** Delete(key1)** event, the record for **key1** is shown in the target table after the batch is processed. This is because all the Delete events are processed before Insert events as per the event processing order explained [above](#conflationpro).</br>In such cases, you should enable the Conflation by setting the `conflation` property to `true`. Now, if a batch contains **Insert(key1)** event followed by a **Delete(key1)** event, then ** SnappyData Sink** conflates these two events into a single event by selecting the last event which is **Delete(key1)** and only that event is processed for **key1**.</br>Processing **Delete(key1)** event without processing **Insert(key1)** event do not result in a failure, as Delete events are ignored, if corresponding records do not exist in the target table.

!!!Note 
	Applications can override the default **SnappyData Sink** semantics by explicitly implementing the [**sinkCallback**](#snappycallback).

## Limitations
Limitations of **SnappyData Sink** are as follows:

*	When the data coming from the source is not partitioned by key columns, then using **SnappyData Sink** may result in inconsistent data. This is because each partition independently processes the data using the [above-mentioned logic](#event_order).

*	When key columns are not defined on the target table and the input dataframe does not contain `_eventType` column, then **SnappyData Sink** cannot guarantee idempotent behavior. This is because inserts cannot be converted into **put into**, as there are no key columns on the table. In such a scenario, **SnappyData Sink** may insert duplicate records after an abrupt failure of the streaming job.

*	The default **SnappyData Sink** implementation does not support partial records for updates. Which means that there is no support to merge updates on a few columns into the store. For all update events, the incoming records must provide values into all the columns of the target table.