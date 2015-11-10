package io.snappydata.app.streaming

import java.util.ArrayList

import io.snappydata.SnappyFunSuite
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

/**
 * Created by ymahajan on 25/09/15.
 */
class StreamingAPISuite extends SnappyFunSuite with Eventually with BeforeAndAfter {

  private var sc: SparkContext = _

  private var ssc: StreamingContext = _

  private var snsc: StreamingSnappyContext = _

  def framework: String = this.getClass.getSimpleName

  def master: String = "local[2]"

  def batchDuration: Duration = Seconds(1)

  val sparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)

  def beforeFunction(): Unit = {
    sc = new SparkContext(sparkConf)
    ssc = new StreamingContext(sc, batchDuration)
    snsc = StreamingSnappyContext(ssc);
    System.out.println("beforeFunction", ssc)
    //if (true) throw new IllegalStateException("ghar ja")
  }

  def afterFunction(): Unit = {
    System.out.println("afterFunction", ssc)
    if (ssc != null) {
      ssc.stop()
    }
  }

  before(beforeFunction)
  after(afterFunction)

/*  test("api stream to stream join") {

    val rdd1 = sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"$i"))
    val stream1 = snsc.createSchemaDStream(
      new ConstantInputDStream[Tweet](ssc, rdd1))
    snsc.registerStreamAsTable("tweet1", stream1)

    val rdd2 = sc.parallelize(1 to 10).map(i => Tweet(i / 5, s"$i"))
    val stream2 = snsc.createSchemaDStream(
      new ConstantInputDStream[Tweet](ssc, rdd2))
    snsc.registerStreamAsTable("tweet2", stream2)

    //val resultList = new ArrayList[String]()
    snsc.registerCQ("SELECT * FROM tweet1 JOIN tweet2 ON tweet1.id = tweet2.id ")
      .foreachRDD { rdd => rdd.foreach(println) }
    //.foreachRDD { rdd => rdd.foreach{row => resultList.add(row.mkString(","))}}
    //println("RESULT ", resultList)

    ssc.start
    ssc.awaitTerminationOrTimeout(10 * 1000)
  }*/

  test("api stream to table join") {
    val rdd1 = sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"$i"))
    val stream1 = snsc.createSchemaDStream(
      new ConstantInputDStream[Tweet](ssc, rdd1))
    snsc.registerStreamAsTable("tweet1", stream1)

    val df2 = snsc.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 5, s"$i")))
    df2.registerTempTable("tweet2")

    //val resultList = new ArrayList[String]()
    snsc.registerCQ("SELECT * FROM tweet1 JOIN tweet2 ON tweet1.id = tweet2.id ")
      .foreachRDD { rdd => rdd.foreach(println) }
    //.foreachRDD { rdd => rdd.foreach{row => resultList.add(row.mkString(","))}}

    ssc.start
    ssc.awaitTerminationOrTimeout(10 * 1000)
  }

/*  ignore("more api") {

    val schema = StructType(Array(StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    val dStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_2)

    val schemaDStream = snsc.createSchemaDStream(dStream.asInstanceOf[DStream[Any]], schema)
    schemaDStream.registerAsTable("people")

    val resultDStream: SchemaDStream = snsc.registerCQ("SELECT name FROM people window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    resultDStream.foreachRDD {
      r => r.foreach(print)
    }
  }*/
}

case class Tweet(id: Int, text: String)
