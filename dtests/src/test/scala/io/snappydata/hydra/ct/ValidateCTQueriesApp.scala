package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, SparkConf}

object ValidateCTQueriesApp {
  val conf = new SparkConf().
      setAppName("ValidateCTQueriesApp Application")
  val sc = new SparkContext(conf)
  val snc = SnappyContext(sc)

  def main(args: Array[String]) {
    CTQueries.snc = snc
    val tableType = args(0)
    val threadID = Thread.currentThread().getId
    val outputFile = "ValidateCTQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    pw.println(s"Validation for queries with ${tableType} tables started")
    CTTestUtil.executeQueries(snc, tableType, pw)
    pw.println(s"Validation for queries with ${tableType} tables completed successfully")
    pw.close()
  }
}
