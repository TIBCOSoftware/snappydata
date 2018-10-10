<a id="howto-streams"></a>

SnappyData supports both the older Spark Streaming model (based on DStreams) as well as the newer Structured streaming model. Unlike, Spark streaming DStreams model(based on RDDs), SnappyData supports Spark SQL in both models. 

# How to use Stream Processing with SnappyData (on top of DStreams) 
SnappyData’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and to integrate with the built-in store. In SnappyData, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate SnappyData tables based on the streaming data. For more information on streaming, refer to this [section](../programming_guide/stream_processing_using_sql.md).

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

## Structured Streaming with SnappyData 

The SnappyData structured streaming programming model is the same as [Spark structured streaming](https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html). 

The only difference is support for ingesting streaming dataframes into SnappyData tables through a built-in '**Sink**'. The **Sink** supports idempotent writes(ensuring consistency of data when failures occur) as well as support for all mutation operations - inserts, appends, updates, puts and deletes. 

The output data source name for SnappyData is `snappysink`.

A minimal code example for structured streaming with SnappyData is available [here](https://github.com/SnappyDataInc/snappydata/blob/streaming_sink/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingExample.scala).
## NOTE: (Jags) above link doesn't work.

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

**SnappyData specific options**

| Options | Description |
|--------|--------|
|    `streamQueryId`    | This is internally used by SnappyData to track the progress of a stream query. The value of this property must be kept unique for each stream query across the SnappyData cluster.  The property is case-insensitive and is mandatory.|
|`tableName`|Name of the SnappyData table where the streaming data is ingested. The property is case-insensitive and is mandatory.|
|`sinkCallback`|This is an optional property which is used to override default **SnappyData Sink** behavior.|

## NOTE: (Jags) where is this sinkCallback described? what is its purpose ?  

### Dealing with Inserts, Updates and Deletes

A common use case for streaming is capturing writes into another store (e.g. a operational database like RDB or NoSQL DB) and streaming the events through Kafka, applying Spark transformations and ingesting into a Analytics datastore (e.g.SnappyData). This pattern is commonly referred to as Change-Data-Capture (CDC). 

To support this use case, SnappyData's Sink supports events to signal if they are Inserts, updates or deletes. The application is required to inject a column called `_eventType` as described below. 
An example explaining the same is available [here](https://github.com/SnappyDataInc/snappydata/blob/streaming_sink/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingCDCExample.scala).
## NOTE: (Jags) link above doesn't work

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

- In a target table with key columns/primary key defined, the **_ put into** operation is applied to all events.
- In a target table without key columns/primary key defined, the **insert** operation is applied to all the events.

### Event ordering

**SnappyData Sink** performs the operations on a streaming batch in the following order with **each partition**:

  - All delete events (deletes relevant records from target table) are processed.
  - All insert events (inserts relevant records into target table) are processed
  - All update events (applies **put into** operation) are processed.

#### Note
It is important to note that this above ordering is done within each partition indepedent of others. This typically means that the input DataFrame should be partitioned on the key. So, all change records on the same key is processed by a single partition (task). If the input source is Kafka, it is advised to maintain this ordering within Kafka and avoiding the shuffle within your Spark app.

To override the default behavior, client codes should implement `SnappySinkCallback` trait and pass the fully qualified name of the implementing class against `sinkCallback` option while defining stream query.

#### Limitation
The current ordering semantics only ensures consistency when incoming events in a batch are for unique keys. So, for instance, a Insert(k1) followed by a Delete(k1) will result in k1 showing up in the target table. For such cases, you should use the `SnappySinkCallback` to override this behavior. We will continue to evolve our semantics to preserve arrival order in the future. 
