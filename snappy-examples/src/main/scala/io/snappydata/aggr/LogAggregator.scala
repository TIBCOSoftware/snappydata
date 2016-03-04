// scalastyle:ignore

package io.snappydata.aggr

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.{SnappyStreamingContext, StreamToRowsConverter}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.unsafe.types.UTF8String

object LogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
      .setAppName("logAggregator")
      // .set("snappydata.store.locators", "localhost:10101")
      // .setMaster("snappydata://localhost:10334")
      .setMaster("local[2]")
  // .set("snappydata.embedded", "true")

  val sc = new SparkContext(sparkConf)
  val ssnc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(30000))

  ssnc.sql("create stream table impressionlog (timestamp long, publisher string," +
      " advertiser string, " +
      "website string, geo string, bid double, cookie string) " +
      "using directkafka_stream options " +
      "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "rowConverter 'io.snappydata.aggr.KafkaStreamToRowsConverter' ," +
      " kafkaParams 'metadata.broker.list->localhost:9092'," +
      " topics 'adnetwork-topic'," +
      " key 'java.lang.String'," +
      " value 'io.snappydata.aggr.ImpressionLog', " +
      " keyDecoder 'kafka.serializer.StringDecoder', " +
      " valueDecoder 'io.snappydata.aggr.ImpressionLogAvroDecoder')")


  val tableStream = ssnc.getSchemaDStream("impressionlog")
  tableStream.foreachRDD(print(_))

  tableStream.foreachDataFrame( df => {
    df.write.format("column").mode(SaveMode.Append)
        .options(Map.empty[String, String]).saveAsTable("gemxdColumnTable")
  })
  // start rolling!
  ssnc.sql("STREAMING START")

  ssnc.awaitTerminationOrTimeout(1800* 1000)

  ssnc.sql("select count(*) from gemxdColumnTable").show()

  ssnc.sql("STREAMING STOP")

  Thread.sleep(20000)

}


class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val log = message.asInstanceOf[ImpressionLog]
    Seq(InternalRow.fromSeq(Seq(log.getTimestamp,
      UTF8String.fromString(log.getPublisher.toString),
      UTF8String.fromString(log.getAdvertiser.toString),
      UTF8String.fromString(log.getWebsite.toString),
      UTF8String.fromString(log.getGeo.toString),
      log.getBid,
      UTF8String.fromString(log.getCookie.toString))))
  }
}