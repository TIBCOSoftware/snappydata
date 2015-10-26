package io.snappydata.app

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.StreamingSnappyContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by ymahajan on 26/10/15.
 */
class StreamSamplingSuite extends FunSuite with Eventually with BeforeAndAfter{

  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var snsc: StreamingSnappyContext = null

  def beforeFunction(): Unit = {
    val conf = new org.apache.spark.SparkConf().
      setMaster("local[2]").
      setAppName("streamsampling")
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
  test("sql stream sampling") {

  }
}
