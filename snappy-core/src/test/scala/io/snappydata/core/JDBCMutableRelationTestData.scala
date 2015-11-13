package io.snappydata.core


import scala.reflect.io.{Path, File}

import io.snappydata.Prop

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext}

/**
  * Test data and test context for Snappy store tests
  */
case class TestData(key1: Int, value: String)

case class TestData2(key1: Int, value: String, ref: Int)

case class Data(col1: Int, col2: Int, col3: Int)

case class Data1(pk: Int, sk: String)

case class Data2(pk: Int, Year: Int)

case class RefData(ref: Int, description: String)

object FileCleaner {
  def cleanFile(path: String): Unit = {
    val file = File(Path(path))
    if (file.exists) file.deleteRecursively()
  }

  def cleanStoreFiles(): Unit = {
    FileCleaner.cleanFile("./metastore_db")
    FileCleaner.cleanFile("./datadictionary")
    FileCleaner.cleanFile("./warehouse")
  }
}

/** A SQLContext that can be used for local testing. */
class LocalSQLContext
    extends SQLContext(
      new SparkContext(
        "local[2]",
        "TestSQLContext",
        new SparkConf()
            .set("spark.sql.unsafe.enabled", "false")
            .set(Prop.Store.locators, "localhost")


      )) {

}

object TestSqlContext {
  def newSparkContext: SparkContext = new LocalSQLContext().sparkContext
}


