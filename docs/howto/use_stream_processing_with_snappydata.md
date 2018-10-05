<a id="howto-streams"></a>
# How to use Stream Processing with SnappyData
SnappyData’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and to integrate with the built-in store. In SnappyData, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate SnappyData tables based on the streaming data. For more information on streaming, refer to the [documentation](../programming_guide/stream_processing_using_sql.md).

**Code Example**: </br>
Code example for streaming is in [StreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StreamingExample.scala). The code snippets below show how to declare a stream table, register continuous queries(CQ) and update SnappyData table using the stream data.

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
      " cookie string) " + " using directkafka_stream options(" +
      " rowConverter 'org.apache.spark.examples.snappydata.RowsConverter'," +
      s" kafkaParams 'metadata.broker.list->$address;auto.offset.reset->smallest'," +
      s" topics 'kafka_topic')"
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

## Structured Streaming with SnappyData 

SnappyData provides default **Sink** implementation for structured streaming which makes it simple to ingest streaming data into SnappyData tables. This default **Sink** implementation uses SnappyData specific format to output data into the target applications. The output data source name for SnappyData is `snappysink`.

A minimal code example for structured streaming with SnappyData is available [here](https://github.com/SnappyDataInc/snappydata/blob/streaming_sink/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingExample.scala).

The following code snippet, from the example, explains the usage of SnappyData's ** Structured Streaming API**:

```pre
    val streamingQuery = structDF
        .filter(_.signal > 10)
        .writeStream
        .format("snappysink")
        .queryName("Devices")
        .trigger(ProcessingTime("1 seconds"))
        .option("streamQueryId", "Devices")     // must be unique across a snappydata cluster
        .option("tableName", "devices")
        .option("checkpointLocation", checkpointDirectory)
        .start()
```

**SnappyData specific options**

| Options | Description |
|--------|--------|
|    `streamQueryId`    | This is internally used by SnappyData to track the progress of a stream query. The value of this option must be kept unique for each stream query across the SnappyData cluster.  The option is case-insensitive and should be mandatorily specified.|
|`tableName`|Name of the SnappyData table where the streaming data is ingested. This should be mandatorily specified.|
|`sinkCallback`|This is an optional property which is used to override default **SnappyData Sink** behaviour.|

### CDC with SnappyData Structured Streaming

**SnappyData's Sink** implementation also supports CDC (Change Data Capture) use cases. An example explaining the same is available [here](https://github.com/SnappyDataInc/snappydata/blob/streaming_sink/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingCDCExample.scala).
To support CDC, the source dataframe must have the following:

*	An `IntegerType` column with name `_eventType`.  The value in the `_eventType` column can be any of the following:
  	-	`0` for insert events  
	-	`1` for update events  
	-	`2` for delete events
	
	In case the input data is following a different convention for event types, then it must be transformed to match the above mentioned format.
    
    !!!Note
    	Records which have `_eventType` value other than the above mentioned ones are skipped.
    
*	The target SnappyData table must have key columns defined for a column table or primary key defined for a row table.

If the `_eventType` column is not provided as part of source dataframe, then the following is observed:

- In a target table with key columns/primary key defined, the **_ put into**
operation will be applied for all events.
- In a target table without key columns/primary key defined, the **insert** operation
is applied for all the events.

### Default Sink Behavior

**SnappyData Sink** allots the operations on a streaming batch in the following order:

  - All delete events (deletes relevant records from target table) are processed.
  - All insert events (inserts relevant records into target table) are processed
  - All update events (applies **put into** operation) are processed.

To override the default behavior, client codes should implement `SnappySinkCallback` trait and pass the fully qualified name of the implementing class against `sinkCallback` option while defining stream query.

#### Limitation

The default sink behavior implies that if a single streaming batch contains multiple events with same key columns/primary key, then delete events for that key columns/primary key is processed first, followed by insert events, and update events irrespective of their order of arrival.
