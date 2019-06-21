
package io.snappydata.hydra.streaming_sink

import java.io.{File, PrintWriter}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.{DataStreamWriter, ProcessingTime}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SnappySession}

object StructuredStreamingTestUtil {

  def structFields_persoon(withEventTypeColumn: Boolean = true): List[StructField] = {
    StructField("id", LongType, nullable = false) ::
        StructField("firstName", StringType, nullable = true) ::
        StructField("middleName", StringType, nullable = true) ::
        StructField("lastName", StringType, nullable = true) ::
        StructField("title", StringType, nullable = true) ::
        StructField("address", StringType, nullable = true) ::
        StructField("country", StringType, nullable = true) ::
        StructField("phone", StringType, nullable = true) ::
        StructField("dateOfBirth", StringType, nullable = true) ::
        StructField("age", IntegerType, nullable = true) ::
        StructField("status", StringType, nullable = true) ::
        StructField("email", StringType, nullable = true) ::
        StructField("education", StringType, nullable = true) ::
        StructField("occupation", StringType, nullable = true) ::
        (if (withEventTypeColumn) {
          StructField("_eventType", IntegerType, nullable = false) :: Nil
        } else {
          Nil
        })
  }

  def getStreamingDF(snc: SnappySession, broker: String, topic: String) : DataFrame = {
    return snc
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
  }

  def getSchema(tableName: String) : StructType = {
    tableName match {
      case "persoon" => return StructType(structFields_persoon())
    }
  }

  def createAndStartStreamingQuery(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File("..")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 15) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9).toInt, r(10),
              r(11), r(12), r(13), r(14).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9).toInt, r(10),
              r(11), r(12), r(13))
          }
        }).writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

  def createAndStartAggStreamingQuery(snc: SnappySession, tableName: String, broker: String,
      topic: String, tid: Int, pw: PrintWriter, withEventTypeColumn: Boolean,
      isConflationTest: Boolean, useCustomCallback: Boolean, outputMode: String): Any = {
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    val streamingDF = getStreamingDF(snc, broker, topic)
    val checkpointDirectory: String = (new File("..")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tid

    val schema = getSchema(tableName)
    implicit val encoder = RowEncoder(schema)
    import snc.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 15) {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9).toInt, r(10),
              r(11), r(12), r(13), r(14).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9).toInt, r(10),
              r(11), r(12), r(13))
          }
        }).groupBy("id").count()
        .writeStream
        .format("snappysink")
        .queryName(s"Query_$tid")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", "Query_" + tid)
        .option("checkpointLocation", checkpointDirectory)

    streamWriter.outputMode(outputMode) // update or complete
    if (useCustomCallback) {
      streamWriter.option("sinkCallback", "")
    }
    if (isConflationTest) {
      pw.println("This is test with conflation enabled.")
      streamWriter.option("conflation", "true")
    }
    streamWriter.start.awaitTermination()
  }

}
