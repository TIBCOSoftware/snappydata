package io.snappydata.app.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}


/**
 * Created by ymahajan on 25/09/15.
 */
class StreamingAPISuite extends FunSuite with Eventually with BeforeAndAfter {

  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var snsc: StreamingSnappyContext = null

  def beforeFunction(): Unit = {
    val conf = new org.apache.spark.SparkConf().
      setMaster("local[2]").
      setAppName("streamingapi")
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Duration(10000))
    snsc = StreamingSnappyContext(ssc);
  }

  def afterFunction(): Unit = {
    if (ssc != null) {
      ssc.stop()
    }
  }

  before(beforeFunction)
  after(afterFunction)

  test("api socket streams") {
    val schema = StructType(Array(StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    val dStream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_2)

    val schemaDStream = snsc.createSchemaDStream(dStream.asInstanceOf[DStream[Any]], schema)
    schemaDStream.registerAsTable("people")

   // val resultDStream: SchemaDStream = snsc.registerCQ("SELECT name FROM people")
  val resultDStream: SchemaDStream = snsc.registerCQ("SELECT name FROM people window (duration '10' seconds, slide '10' seconds) WHERE age >= 18")

    //resultDStream.window(Duration(10000))
    resultDStream.foreachRDD {
      r => r.foreach(print)
    }
    ssc.start
    ssc.awaitTerminationOrTimeout(5 * 1000)
  }
}