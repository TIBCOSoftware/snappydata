// scalastyle:ignore

package io.snappydata.aggr

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.{SnappyStreamingContext, StreamToRowsConverter}
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.unsafe.types.UTF8String

object LogAggregator extends App {

  val sparkConf = new org.apache.spark.SparkConf()
      .setAppName("logAggregator")
      .setMaster("local[2]")
  // .setMaster("snappydata://localhost:10334")

  val sc = new SparkContext(sparkConf)
  val ssnc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(10000))

  ssnc.sql("create stream table impressionlog (timestamp long, publisher string," +
      " advertiser string, " +
      "website string, geo string, bid double, cookie string) " +
      "using directkafka_stream options " +
      "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "rowConverter 'io.snappydata.aggr.KafkaStreamToRowsConverter' ," +
      " kafkaParams 'metadata.broker.list->localhost:9092,localhost:9093'," +
      " topics 'adnetwork-topic'," +
      " K 'java.lang.String'," +
      " V 'io.snappydata.aggr.ImpressionLog', " +
      " KD 'kafka.serializer.StringDecoder', " +
      " VD 'io.snappydata.aggr.ImpressionLogAvroDecoder')")

  ssnc.sql("create table snappyStoreTable(timestamp long, publisher string," +
      " geo string, avg_bid double) " +
      "using column " +
      "options(PARTITION_BY 'timestamp')")

  ssnc.registerCQ("select timestamp, publisher, geo, avg(bid) as avg_bid" +
      " from impressionlog window (duration '10' seconds, slide '10' seconds)" +
      " where geo != 'unknown' group by publisher, geo, timestamp")
      .foreachDataFrame(df => {
        df.show
        df.write.format("column").mode(SaveMode.Append)
            .options(Map.empty[String, String]).saveAsTable("snappyStoreTable")
      })
  ssnc.sql("STREAMING START")

  ssnc.awaitTerminationOrTimeout(1800 * 1000)

  ssnc.sql("select count(*) from snappyStoreTable").show()

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