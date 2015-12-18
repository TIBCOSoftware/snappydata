package io.snappydata.app.streaming

import io.snappydata.SnappyFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.{SchemaDStream, StreamToRowsConverter, StreamingSnappyContext}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import twitter4j.{Status, TwitterObjectFactory}

import scala.collection.mutable.Queue

/**
  * Created by ymahajan on 25/09/15.
  */
class StreamingSuite extends SnappyFunSuite with Eventually with BeforeAndAfter {

  private var ssc: StreamingContext = _

  private var streamingSnappy: StreamingSnappyContext = _

  def framework: String = this.getClass.getSimpleName

  def master: String = "local[2]"

  def batchDuration: Duration = Seconds(1)

  before {
    ssc = new StreamingContext(sc, batchDuration)
    streamingSnappy = StreamingSnappyContext(ssc);
    // ssc.checkpoint("/tmp")
  }

  after {
    if (streamingSnappy != null) {
      StreamingSnappyContext.stop()
    }
  }

  test("save stream to external table using forEachDataFrame") {
    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssc.queueStream[Tweet](getQueueOfRDDs1)
    val map = Map.empty[String,String]
    val schemaStream1 = streamingSnappy.createSchemaDStream(dStream1)
    streamingSnappy.dropExternalTable("gemxdColumnTable1", true)
    schemaStream1.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String,String]).saveAsTable("gemxdColumnTable1")
    })

    streamingSnappy.dropExternalTable("gemxdColumnTable2", true)
    schemaStream1.foreachDataFrame((df,time) => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String,String]).saveAsTable("gemxdColumnTable2")
    })

    ssc.start
    ssc.awaitTerminationOrTimeout(20 * 1000)

    val result1 = streamingSnappy.sql("select * from gemxdColumnTable1")
    val r1 = result1.collect
    assert(r1.length == 30)

    val result2 = streamingSnappy.sql("select * from gemxdColumnTable2")
    val r2 = result2.collect
    assert(r2.length == 30)
  }

  test("SNAP-240 NotSerializableException with checkpoint") {

    def getQueueOfRDDs: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    import org.apache.spark.sql.snappy._

    val dStream = ssc.queueStream[Tweet](getQueueOfRDDs)

    val schemaDStream = streamingSnappy.createSchemaDStream(dStream)
    schemaDStream.foreachRDD(rdd => {
      // streamingSnappy.createDataFrame
      // (rdd, schemaDStream.schema).show() //NotSerializableException
      // println(rdd) // scalastyle:ignore
    })

    ssc.start
    ssc.awaitTerminationOrTimeout(5 * 1000)
  }

  test("api stream to stream and stream to table join") {
    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = streamingSnappy.createSchemaDStream(dStream1)
    schemaStream1.foreachRDD(rdd => {
      streamingSnappy.createDataFrame(rdd, schemaStream1.schema)
    })
    streamingSnappy.registerStreamAsTable("tweetStream1", schemaStream1)

    def getQueueOfRDDs2: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = streamingSnappy.createSchemaDStream(dStream2)
    schemaStream2.foreachRDD(rdd => {
      streamingSnappy.createDataFrame(rdd, schemaStream2.schema)
    })
    streamingSnappy.registerStreamAsTable("tweetStream2", schemaStream2)

    val resultStream: SchemaDStream = streamingSnappy.registerCQ("SELECT t1.id, t1.text FROM " +
      "tweetStream1 window (duration '2' seconds, slide '2' seconds) t1 JOIN " +
      "tweetStream2 t2 ON t1.id = t2.id ")

    streamingSnappy.dropExternalTable("gemxdColumnTable", true)
    streamingSnappy.createExternalTable("gemxdColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachRDD(rdd => {
      val df = streamingSnappy.createDataFrame(rdd, schemaStream1.schema)
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })

    val df = streamingSnappy.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.registerTempTable("tweetTable")

    val resultSet = streamingSnappy.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window " +
      "(duration '4' seconds, slide '4' seconds) " +
      "t1 JOIN tweetTable t2 ON t1.id = t2.id")
    resultSet.foreachRDD(rdd => {
      val df = streamingSnappy.createDataFrame(rdd, schemaStream1.schema)
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })

    ssc.start
    ssc.awaitTerminationOrTimeout(20 * 1000)

    val result = streamingSnappy.sql("select * from gemxdColumnTable")
    val r = result.collect
    assert(r.length > 0)
  }

  ignore("dynamic CQ") {

    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }
    val dStream1 = ssc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = streamingSnappy.createSchemaDStream(dStream1)
    schemaStream1.foreachRDD(rdd => {
      streamingSnappy.createDataFrame(rdd, schemaStream1.schema)
    })
    streamingSnappy.registerStreamAsTable("tweetStream1", schemaStream1)

    def getQueueOfRDDs2: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = streamingSnappy.createSchemaDStream(dStream2)
    schemaStream2.foreachRDD(rdd => {
      streamingSnappy.createDataFrame(rdd, schemaStream2.schema)
    })
    streamingSnappy.registerStreamAsTable("tweetStream2", schemaStream2)

    val resultStream: SchemaDStream = streamingSnappy.registerCQ("SELECT t1.id, t1.text" +
      " FROM tweetStream1 window (duration '2' seconds, slide '2' seconds)" +
      "t1 JOIN tweetStream2 t2 ON t1.id = t2.id ")

    streamingSnappy.dropExternalTable("gemxdColumnTable", true)
    streamingSnappy.createExternalTable("gemxdColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachRDD(rdd => {
      val df = streamingSnappy.createDataFrame(rdd, schemaStream1.schema)
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })

    val df = streamingSnappy.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.registerTempTable("tweetTable")

    ssc.start

    val resultSet = streamingSnappy.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window" +
      " (duration '4' seconds, slide '4' seconds) " +
      "t1 JOIN tweetTable t2 ON t1.id = t2.id")

    resultSet.foreachRDD(rdd => {
      val df = streamingSnappy.createDataFrame(rdd, schemaStream1.schema)
      df.show
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })
    ssc.awaitTerminationOrTimeout(20 * 1000)

    val result = streamingSnappy.sql("select * from gemxdColumnTable")
    val r = result.collect
    assert(r.length > 0)
  }

  test("sql on socket streams") {

    streamingSnappy.sql("create stream table socketStreamTable (name string) " +
      "using socket_stream options (" +
      "hostname 'localhost', " +
      "port '9998', " +
      "storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "streamToRows 'io.snappydata.app.streaming.LineToRowsConverter') ")

    streamingSnappy.registerCQ("SELECT * FROM socketStreamTable window " +
      "(duration '10' seconds, slide '10' seconds) ")

    val thrown = intercept[Exception] {
      streamingSnappy.sql( """STREAMING CONTEXT START """)
    }
    assert(thrown.getMessage === "requirement failed: No output operations" +
      " registered, so nothing to execute")
    ssc.awaitTerminationOrTimeout(10000)
  }

  test("sql on kafka streams") {

    streamingSnappy.sql("create stream table directKafkaStreamTable (name string, age int)" +
      " using kafka_stream options " +
      "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "streamToRows 'io.snappydata.app.streaming.KafkaStreamToRowsConverter', " +
      "zkQuorum 'localhost:2181', " +
      "groupId 'streamSQLConsumer', " +
      "topics 'tweets:01')")

    /* val tableDStream: SchemaDStream = streamingSnappy.getSchemaDStream("directKafkaStreamTable")
    import org.apache.spark.sql.streaming.snappy._
    tableDStream.saveToExternalTable("kafkaStreamGemXdTable", tableDStream.schema,
   Map.empty[String, String]) */

    val thrown = intercept[Exception] {
      streamingSnappy.sql( """STREAMING CONTEXT START """)
    }
    assert(thrown.getMessage === "requirement failed: No output operations " +
      "registered, so nothing to execute")
    ssc.awaitTerminationOrTimeout(10000)
  }


  test("sql on direct kafka streams") {

    intercept[Exception] {
      // java.nio.channels.ClosedChannelException since no kafka cluster
      streamingSnappy.sql("create stream table directKafkaStreamTable (name string, age int) " +
        "using directkafka_stream options " +
        "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
        "streamToRows 'io.snappydata.app.streaming.KafkastreamToRowsConverter', " +
        " kafkaParams 'metadata.broker.list->localhost:9092', " +
        "topics 'tweets')")
    }

    val ex = intercept[Exception] {
      streamingSnappy.sql( """STREAMING CONTEXT START """)
    }
    assert(ex.getMessage === "requirement failed: No output operations" +
      " registered, so nothing to execute")
  }

  test("sql on file streams") {

    // var hfile: String = getClass.getResource("/2015.parquet").getPath
    streamingSnappy.sql("create stream table fileStreamTable (name string, age int)" +
      " using file_stream options " +
      "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "streamToRows 'io.snappydata.app.streaming.KafkaStreamToRowsConverter', " +
      " directory '/tmp')")
    streamingSnappy.registerCQ("SELECT name FROM fileStreamTable window " +
      "(duration '10' seconds, slide '10' seconds) WHERE age >= 18")
    val thrown = intercept[Exception] {
      streamingSnappy.sql( """STREAMING CONTEXT START """)
    }
    assert(thrown.getMessage === "requirement failed: No output operations" +
      " registered, so nothing to execute")
    ssc.awaitTerminationOrTimeout(5000)
  }
}

case class Tweet(id: Int, text: String)

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(InternalRow.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.mkString(",")))))
  }

}

class LineToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    Seq(InternalRow.fromSeq(Seq(UTF8String.fromString(message.toString))))
  }
}

class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    TwitterObjectFactory.getRawJSON(message)
    Seq(InternalRow.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.mkString(",")))))
  }

  /* override def toRow(message: Any): Seq[InternalRow] = {
    // TODO Yogesh. convert this raw JSON string to twitter4j.SatusJSONImpl
    val status : Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    // val tweet = new JSONObject(message.asInstanceOf[String])
    val hashTags = status.getHashtagEntities

    val limit = KafkaMessageToRowConverter.rand.nextInt(20000)
    (0 until limit).flatMap { i =>
      val id = status.getId + (i.toLong *
        KafkaMessageToRowConverter.rand.nextInt(100000))
      if (hashTags.length <= 1) {
        Seq(InternalRow.fromSeq(Seq(id,
          UTF8String.fromString(status.getText),
          UTF8String.fromString(status.getUser().getName),
          UTF8String.fromString(status.getUser.getLang),
          status.getRetweetCount,
          UTF8String.fromString(if (hashTags.isEmpty) "" else hashTags(0).getText))))
      } else {
        hashTags.map { tag =>
          InternalRow.fromSeq(Seq(id,
            UTF8String.fromString(status.getText),
            UTF8String.fromString(status.getUser().getName),
            UTF8String.fromString(status.getUser.getLang),
            status.getRetweetCount, UTF8String.fromString(tag.getText)))
        }
      }
    }
    // Row.fromSeq
  }  */
}

/*
object KafkaMessageToRowConverter {
  private val rand = new Random
} */