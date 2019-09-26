<a id="howto-streams"></a>

# How to use Stream Processing with TIBCO ComputeDB 

TIBCO ComputeDB supports both the older [Spark Streaming model (based on DStreams)](#dstreams) as well as the newer [Structured Streaming model](#structuredstreaming). Unlike the Spark streaming DStreams model, that is based on RDDs, TIBCO ComputeDB supports Spark SQL in both models.

<a id= structuredstreaming> </a>
## Structured Streaming

The TIBCO ComputeDB structured streaming programming model is the same as [Spark structured streaming](https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html). The only difference is support for ingesting streaming dataframes into TIBCO ComputeDB tables through a built-in **Sink**. 

TIBCO ComputeDB provides a build-in output **Sink** which simplifies ingestion of streaming dataframes into TIBCO ComputeDB tables. The **Sink** supports idempotent writes, ensuring consistency of data when failures occur, as well as support for all mutation operations such as inserts, appends, updates, puts, and deletes.

The output data source name for SnappyData is `snappysink`. A minimal code example for structured streaming with socket source and **Snappy Sink** is available [here](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/SocketSourceExampleWithSnappySink.scala). You can also refer to [Structured Streaming Quickstart guide](/quickstart/structucture_streamingquickstart.md). You can also refer to [Structured Streaming Quickstart guide](/quickstart/structucture_streamingquickstart.md). 

For more examples, refer to [structured streaming examples](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming). The following examples are shown:

| Example | Description |
|--------|--------|
|    [CDCExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/CDCExample.scala)   |   An example explaining CDC (change data capture) use case with SnappyData streaming Sink. |
|  [CSVFileSourceExampleWithSnappySink.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/CSVFileSourceExampleWithSnappySink.scala)      |   An example of structured streaming depicting CSV file processing with Snappy Sink.    |
|  [CSVKafkaSourceExampleWithSnappySink.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/CSVKafkaSourceExampleWithSnappySink.scala)    | An example of structured streaming depicting processing of JSON coming from kafka source using snappy Sink.       |
|   [JSONFileSourceExampleWithSnappySink.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/JSONFileSourceExampleWithSnappySink.scala)   |     An example of structured streaming depicting JSON file processing with Snappy Sink.   |
|   [JSONKafkaSourceExampleWithSnappySink.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/JSONKafkaSourceExampleWithSnappySink.scala)     |  An example of structured streaming depicting processing of JSON coming from Kafka source using Snappy Sink      |
|    [SocketSourceExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/SocketSourceExample.scala)    |    An example showing usage of structured streaming with console Sink.   |
|      [SocketSourceExampleWithSnappySink.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/SocketSourceExampleWithSnappySink.scala)  |   An example showing usage of structured streaming with SnappyData.    |

The topic included the following sections:

* [Using SnappyData Structured Streaming API](#ssapi)
* [Handling Inserts, Updates and Deletes](#haninsertupdatesdeletes)
* [Event Processing Order](#event_order)
* [Sink State Table](#sinkstatetable)
* [Overriding Default Sink Behavior](#overridesinkbeha)
* [Resetting a Streaming Query](#resetstreamquery)
* [Best Practices for Structured Streaming](#bestpracticesstruc)
* [Limitations](#limitationsstruc)

<a id= ssapi> </a>
### Using TIBCO ComputeDB Structured Streaming API

The following code snippet, from the example, explains the usage of TIBCO ComputeDB's ** Structured Streaming API**:

```pre
    val streamingQuery = structDF
        .filter(_.signal > 10)      // so transformation on input dataframe 
        .writeStream
        .format("snappysink")       // Required to ingest into TIBCO ComputeDB tables
        .queryName("Devices")   // Required when using snappysink. Must be unique across the TIBCO ComputeDB cluster.
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", "devices")     // Required: name of the snappy table where data will be ingested.
        .option("checkpointLocation", checkpointDirectory)
        .start()
```
#### TIBCO ComputeDB Specific options

The following are TIBCO ComputeDB specific options which can be configured for Structured Streaming:

| Options | Description |
|--------|--------|
|`tableName`|Name of the TIBCO ComputeDB table where the streaming data is ingested. The property is case-insensitive and is mandatory.|
|`stateTableSchema`|Name of the schema under which TIBCO ComputeDB’s internal state table will be created. This table is used to track the progress of the streaming queries and enables snappy sink to behave in an idempotent manner when streaming query is restarted after abrupt failures or planned down time.</br>This is a mandatory property when security is enabled for the TIBCO ComputeDB cluster. When security is disabled, snappy sink uses APP schema by default to store the sink state table.|
|`conflation`|This is an optional boolean property with the default value set to `false`. Conflation is enabled only when you set this property to `true`. </br>If this property is set to `true` and if the incoming streaming batch contains multiple events on the same key, **Snappy Sink** automatically reduces this to a single operation. This is typically the last operation on any given key for the batch that is being processed. This property is only applicable when the `_eventType` column is available (see [below](#eventypecolumn)) and the target table has Keys defined. For more information, see [here](#conflationpro). |
|<a id= snappycallback> </a>`sinkCallback`|This is an optional property which is used to override default **Snappy Sink** behavior. To override the default behavior, client codes should implement `SnappySinkCallback` trait and pass the fully qualified name of the implementing class against this property value.|

<a id= haninsertupdatesdeletes> </a>
### Handling Inserts, Updates and Deletes

A common use case for streaming is capturing writes into another store (Operational database such as RDB or NoSQL DB) and streaming the events through Kafka, applying Spark transformations, and ingesting into an analytics datastore such as TIBCO ComputeDB. This pattern is commonly referred to as **Change-Data-Capture (CDC)**.

<a id= eventypecolumn> </a>
To support this use case, **Snappy Sink** supports events to signal if these are Inserts, Updates, or Deletes. The application is required to inject a column called `_eventType` as described below. 

To support **CDC**, the source DataFrame must have the following:

*	An `IntegerType` column named `_eventType`.  The value in the `_eventType` column can be any of the following:
  	-	`0` for insert events
	-	`1` for update events
	-	`2` for delete events

	In case the input data is following a different convention for event types, then it must be transformed to match the above-mentioned format.

    !!!Note
    	Records which have `_eventType` value other than the above-mentioned ones are skipped.

*	The target TIBCO ComputeDB table must have key columns defined for a column table or primary key defined for a row table.

An example explaining the **CDC** use case is available [here](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/structuredstreaming/CDCExample.scala).

If the `_eventType` column is not provided as part of source dataframe, then the following is observed:</br> 

-	In a target table with key columns/primary key defined, the **put into** operation is applied to all events.
-	In a target table without key columns/primary key defined, the **insert** operation is applied to all the events.

<a id= event_order> </a>
### Event Processing Order

Currently, the ordering of events across partitions is not supported. Event processing occurs independently in each partition. Hence, you must ensure that in your application all the events, that are associated with a key, are always delivered on the same partition (shard on the key).

If your incoming stream is not partitioned on the key column, the application should first repartition the dataframe on the key column. You can ignore this requirement, if your incoming streams are continuously appending. For example, time series or when replacing data where ordering is irrelevant.

<a id= abovementioned> </a>
The writes occur by grouping the events in the following manner (for performance optimization of the columnar store) and is only applicable when your DataFrame has an **_eventType** column: 

*	Processes all delete events (deletes relevant records from target table)
*	Processes all insert events (inserts relevant records into the target table
*	Processes all update events (applies PutInto operation)

If the **_eventType** column is not provided as part of source dataframe, then the events are processed in the following manner:

*	If key columns/primary keys are defined for the target table, then all the events are treated as update events and **put into** operation is performed for all events.
*	If key columns/primary keys are not defined for the target table, then all the events are treated as insert events and insert operation is applied for all events.

<a id= conflationpro> </a>
If `conflation` property is set to `true`, **Snappy Sink** will first conflate the incoming batch independently by each partition. The conflation of events is performed in the following steps:

*	Group all the events in the given partition by key.
*	Convert inserts into **put into** operations if the event type of the last event for a key is of insert type and there are more than one events for the same key.
*	Keep the last event for each key and drop remaining events.

This results in a batch, where there is at most a single entry per key. 

By default the `conflation` property is set to `false`. Therefore, the event processing semantics only ensures consistency when incoming events in a batch are for the unique key column(s).

**For example:**</br>If an incoming batch contains an **Insert(key1)** event followed by a **Delete(key1)** event, the record for **key1** is shown in the target table after the batch is processed. This is because all the Delete events are processed before Insert events as per the event processing order explained [here](#abovementioned).
In such cases, you should enable the Conflation by setting the **conflation** property to true. Now, if a batch contains **Insert(key1)** event followed by a Dele**te(key1)** event, then SnappyData Sink conflates these two events into a single event by selecting the last event which is **Delete(key1)** and only that event is processed for **key1**.
Processing **Delete(key1)** event without processing **Insert(key1)** event does not result in a failure, as Delete events are ignored if corresponding records do not exist in the target table.

<a id= sinkstatetable> </a>
### Sink State Table

A replicated row table with name **SNAPPYSYS_INTERNAL____SINK_STATE_TABLE** is created by **Snappy Sink** under schema specified by the **stateTableSchema** option if the table does not exist. If the **stateTableSchema** is not specified then the sink state table is created under the **APP** schema. During the processing of each batch, this state is updated.

This table is used by **Snappy Sink** to maintain the state of the streaming queries. This state is important to maintain the idempotency of the sink In case of stream failures. The **Sink State** table contains the following fields:

| Name | Type |Comment|
|--------|--------|--------|
|  stream_query_id      |  varchar(200)      |Primary Key. Name of the streaming query|
|batch_id|long|Batch id of the most recent batch picked up for processing.|

#### Behavior of Sink State Table in a Secure cluster

When security is enabled for the cluster, the **stateTableSchema** becomes a mandatory option. Also, when you submit the streaming job, you must have the necessary permissions on the schema specified by **stateTableSchema** option.

#### Maintaining Idempotency In Case Of Stream Failures

When stream execution fails, it is possible that the streaming batch was half processed. Hence next time whenever the stream is started, Spark picks the half processed batch again for processing. This can lead to extraneous records in the target table if the batch contains insert events. To overcome this, Snappy Sink keeps the state of a stream query execution as part of the Sink State table. 

!!! Note
	The key columns in a column table are merely a hint (used to perform **put into** and **delete** operations) and does not enforce a unique constraint such as a primary key in case of a row table.

Using this state, Snappy Sink can detect whether a batch is a duplicate batch. If a batch is a duplicate batch then Snappy Sink processes all insert events from the batch using **put into** operation. This ensures that no duplicate records are inserted into the target table.

!!! Note
	The above-mentioned behavior is applicable only when the key columns are defined on the target table as key columns are necessary to apply **put into** operation. When key columns are not defined on the target table, Snappy Sink does not behave in an idempotent manner and it can lead to duplicate records in the target table when the streaming query is restarted after stream failure.

<a id= overridesinkbeha> </a>
### Overriding Default Sink Behavior

If required, applications can override the default **Snappy Sink** semantics by implementing **org.apache.spark.sql.streaming.SnappySinkCallback** and passing the fully qualified name of the implementing class as a value of **sinkCallback** option of **Snappy Sink**.

**SnappySinkCallback** trait contains one method which needs to be implemented by the implementing class. This method is called for each streaming batch after checking the possibility of batch duplication which is indicated by **possibleDuplicate** flag.

A duplicate batch might be picked up for processing in case of failure. In the case of batch duplication, this method should handle batch in an idempotent manner in order to avoid data inconsistency.

```
def process(snappySession: SnappySession, sinkProps: Map[String, String],
      batchId: Long, df: Dataset[Row], possibleDuplicate: Boolean = false): Unit
```

<a id= resetstreamquery> </a>
### Resetting a Streaming Query

Progress of a streaming query is saved as part of the checkpoint directory by Spark. On top of this **Snappy Sink** also maintains an internal state as part of the state table to ensure idempotency of the sink. 


Hence to reset a streaming query, the following actions must be taken to clean the state of the streaming query: 


!!! Note
	When you use the following steps you may permanently lose the state of the streaming query.


1.	Delete the checkpoint directory. (or start streaming query with different checkpoint directory.)
2.	Clear the state from the state table using following sql:</br>
		
        	delete from [state_table_schema].snappysys_internal____sink_state_table where stream_query_id = <query_name>;

	*	`[state_table_schema]` is the schema passed as part of **stateTableSchema** option of snappy sink. It should be skipped if **stateTableSchema** option was not provided while defining snappy sink. 
	*	`<query_name>` is the name of the query provided while defining the sink.

<a id= bestpracticesstruc> </a>
### Best Practices for Structured Streaming 

Refer to the [Best Practices for Structured Streaming](/best_practices/structured_streaming_best_practices.md). 

<a id= limitationsstruc> </a>
### Limitations
Limitations of **Snappy Sink** are as follows:

*	When the data coming from the source is not partitioned by key columns, then using **Snappy Sink** may result in inconsistent data. This is because each partition independently processes the data using the [above-mentioned logic](#event_order).

*	When key columns are not defined on the target table and the input dataframe does not contain `_eventType` column, then **Snappy Sink** cannot guarantee idempotent behavior. This is because inserts cannot be converted into **put into**, as there are no key columns on the table. In such a scenario, **Snappy Sink** may insert duplicate records after an abrupt failure of the streaming job.

*	The default **Snappy Sink** implementation does not support partial records for updates. Which means that there is no support to merge updates on a few columns into the store. For all update events, the incoming records must provide values into all the columns of the target table.

<a id= dstreams> </a>
## Spark Streaming DStreams Model

TIBCO ComputeDB’s streaming functionality builds on top of Spark Streaming and is primarily aimed at making it simpler to build streaming applications and to integrate with the built-in store. In TIBCO ComputeDB, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate TIBCO ComputeDB tables based on the streaming data. For more information on streaming, refer to this [section](../programming_guide/stream_processing_using_sql.md).

### Code Sample

Code example for streaming is in [StreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StreamingExample.scala). The code snippets in the following sections show how to declare a stream table, register continuous queries(CQ), and update TIBCO ComputeDB table using the stream data.

### Using Stream Processing with TIBCO ComputeDB

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

The example starts an embedded Kafka instance on which a few messages are published. TIBCO ComputeDB processes these message and updates a table based on the stream data.

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