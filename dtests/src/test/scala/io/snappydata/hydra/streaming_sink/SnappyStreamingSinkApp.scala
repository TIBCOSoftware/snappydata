
package io.snappydata.hydra.streaming_sink

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object SnappyStreamingSinkApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().
        setAppName("ValidateCTQueriesApp Application_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc).snappySession
    val tid: Int = args(0).toInt
    var brokerList: String = args(1)
    brokerList = brokerList.replace("--", ":")
    val kafkaTopic: String = args(2)
    val tableName: String = args(3)
    val isConflationTest: Boolean = args(4).toBoolean
    val useCustomCallback: Boolean = false // args(5).toBoolean

    val outputFile = "KafkaStreamingApp_output" + tid + "_" + System.currentTimeMillis() + ".txt"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()

    StructuredStreamingTestUtil.createAndStartStreamingQuery(snc, tableName, brokerList,
      kafkaTopic, tid, pw, isConflationTest, true, useCustomCallback)

    pw.println("started streaming query")
    pw.flush()

  }
}
