package io.snappydata.app.streaming

import io.snappydata.SnappyFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.collection.mutable.Queue

/**
 * Created by ymahajan on 25/09/15.
 */
class StreamingAPISuite extends SnappyFunSuite with Eventually with BeforeAndAfterAll {

  private var ssc: StreamingContext = _

  private var snsc: StreamingSnappyContext = _

  def framework: String = this.getClass.getSimpleName

  def master: String = "local[2]"

  def batchDuration: Duration = Seconds(1)

  override def newSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(framework)
    sparkConf
  }

  override def afterAll(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
  }

  override def beforeAll(): Unit = {
    ssc = new StreamingContext(sc, batchDuration)
    //ssc.checkpoint(null)//Duration(60*1000))
    snsc = StreamingSnappyContext(ssc);
  }

  test("api stream to stream and stream to table join") {

    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = snsc.createSchemaDStream(dStream1)
    schemaStream1.foreachRDD(rdd => {
      snsc.createDataFrame(rdd, schemaStream1.schema)
    })
    snsc.registerStreamAsTable("tweetStream1", schemaStream1)

    def getQueueOfRDDs2: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = snsc.createSchemaDStream(dStream2)
    schemaStream2.foreachRDD(rdd => {
      snsc.createDataFrame(rdd, schemaStream2.schema)
    })
    snsc.registerStreamAsTable("tweetStream2", schemaStream2)

    val resultStream: SchemaDStream = snsc.registerCQ("SELECT t1.id, t1.text FROM " +
      "tweetStream1 window (duration '2' seconds, slide '2' seconds) t1 JOIN " +
      "tweetStream2 t2 ON t1.id = t2.id ")

    snsc.dropExternalTable("gemxdColumnTable", true)
    snsc.createExternalTable("gemxdColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachRDD(rdd => {
      val df = snsc.createDataFrame(rdd, schemaStream1.schema)
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })

    val df = snsc.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.registerTempTable("tweetTable")

    val resultSet = snsc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window " +
      "(duration '4' seconds, slide '4' seconds) " +
      "t1 JOIN tweetTable t2 ON t1.id = t2.id")
    resultSet.foreachRDD(rdd => {
      val df = snsc.createDataFrame(rdd, schemaStream1.schema)
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })

    ssc.start
    ssc.awaitTerminationOrTimeout(20 * 1000)

    val result = snsc.sql("select * from gemxdColumnTable")
    val r = result.collect
    assert(r.length == 10)
  }

  ignore("dynamic CQ") {
    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }
    val dStream1 = ssc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = snsc.createSchemaDStream(dStream1)
    schemaStream1.foreachRDD(rdd => {
      snsc.createDataFrame(rdd, schemaStream1.schema)
    })
    snsc.registerStreamAsTable("tweetStream1", schemaStream1)

    def getQueueOfRDDs2: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = snsc.createSchemaDStream(dStream2)
    schemaStream2.foreachRDD(rdd => {
      snsc.createDataFrame(rdd, schemaStream2.schema)
    })
    snsc.registerStreamAsTable("tweetStream2", schemaStream2)

    val resultStream: SchemaDStream = snsc.registerCQ("SELECT t1.id, t1.text FROM tweetStream1" +
      " window (duration '2' seconds, slide '2' seconds) t1 JOIN " +
      "tweetStream2 t2 ON t1.id = t2.id ")

    snsc.dropExternalTable("gemxdColumnTable", true)
    snsc.createExternalTable("gemxdColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachRDD(rdd => {
      val df = snsc.createDataFrame(rdd, schemaStream1.schema)
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    }
    )

    val df = snsc.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.registerTempTable("tweetTable")

    ssc.start

    val resultSet = snsc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window" +
      " (duration '4' seconds, slide '4' seconds) " +
      "t1 JOIN tweetTable t2 ON t1.id = t2.id")

    resultSet.foreachRDD(rdd => {
      val df = snsc.createDataFrame(rdd, schemaStream1.schema)
      df.show
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
        .saveAsTable("gemxdColumnTable")
    })
    ssc.awaitTerminationOrTimeout(20 * 1000)

    val result = snsc.sql("select * from gemxdColumnTable")
    val r = result.collect
    assert(r.length == 10)
    println("successful")
  }
}

case class Tweet(id: Int, text: String)
