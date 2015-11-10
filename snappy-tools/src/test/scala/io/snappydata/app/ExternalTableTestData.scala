package io.snappydata.app

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by rishim on 27/8/15.
 */
case class TestData(key: Int, value: String)
case class Data(col1: Int, col2: Int, col3: Int)
case class Data1(col1: Int, col2: String)
/** A SQLContext that can be used for local testing. */
class LocalSQLContext
  extends SQLContext(
    new SparkContext(
      "local[2]",
      "TestSQLContext",
      new SparkConf().set("spark.sql.testkey", "true"))) {



}


