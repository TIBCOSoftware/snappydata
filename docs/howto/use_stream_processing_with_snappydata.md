<a id="howto-streams"></a>
# How to use Stream Processing with SnappyData
SnappyData’s streaming functionality builds on top of Spark Streaming and primarily is aimed at making it simpler to build streaming applications and to integrate with the built-in store. In SnappyData, you can define streams declaratively from any SQL client, register continuous queries on streams, mutate SnappyData tables based on the streaming data. For more information on streaming, refer to the [documentation](../programming_guide/stream_processing_using_sql.md).

**Code Example **: </br>
Code example for streaming is in [StreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StreamingExample.scala). The code snippets below show how to declare a stream table, register continuous queries(CQ) and update SnappyData table using the stream data.

**First get a SnappySession and a SnappyStreamingContext**: </br>
Here SnappyStreamingContext is initialized in a batch duration of one second.
```no-highlight
    val spark: SparkSession = SparkSession
        .builder
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate

    val snsc = new SnappyStreamingContext(spark.sparkContext, Seconds(1))
```

The example starts an embedded Kafka instance on which a few messages are published. SnappyData processes these message and updates a table based on the stream data.

The SQL below shows how to declare a stream table using SQL. The rowConverter attribute specifies a class used to return Row objects from the received stream messages.
```no-highlight
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

RowsConverter decodes a stream message consisting of comma separated fields and forms a Row object from it.

```no-highlight
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

```no-highlight
snsc.sql("create table publisher_bid_counts(publisher string, bidCount int) using row")
```

**To declare a continuous query that is executed on the streaming data**: This query returns a number of bids per publisher in one batch.

```no-highlight
    val resultStream: SchemaDStream = snsc.registerCQ("select publisher, count(bid) as bidCount from " +
        "adImpressionStream window (duration 1 seconds, slide 1 seconds) group by publisher")
```

**To process that the result of above continuous query to update the row table publisher_bid_counts**:

```no-highlight
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

```no-highlight
snsc.snappySession.sql("select publisher, bidCount from publisher_bid_counts").show()
```

## Spark's structured streaming with SnappyData 

Using SnappyData you can run streaming queries on IoT Device data received from a data server listening on a TCP socket.

**Code Example**:</br>

The complete source code of the example [StructuredStreamingExample.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/StructuredStreamingExample.scala) shows the usage of structured streaming with SnappyData.


