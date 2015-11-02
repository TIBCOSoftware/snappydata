package io.snappydata.core



import scala.reflect.io.{Path, File}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext}

/**
 * Test data and test context for Snappy store tests
 */
case class TestData(key1: Int, value: String)
case class TestData2(key1: Int, value: String, ref : Int)
case class Data(col1: Int, col2: Int, col3: Int)
case class Data1(pk: Int, sk: String)
case class Data2(pk: Int, Year: Int)

object FileCleaner{
  def cleanFile(path : String): Unit ={
    val file = File(Path(path))
    if(file.exists) file.deleteRecursively()
  }

  def cleanStoreFiles(): Unit ={
    FileCleaner.cleanFile("./JdbcRDDSuiteDb")
    FileCleaner.cleanFile("./metastore_db")
    FileCleaner.cleanFile("./datadictionary")
  }
}

/** A SQLContext that can be used for local testing. */
class LocalSQLContext
  extends SQLContext(
    new SparkContext(
      "local[2]",
      "TestSQLContext",
      new SparkConf().set("spark.sql.testkey", "true")
         .set("snappy.store.jdbc.url","jdbc:gemfirexd:;mcast-port=33619;user=app;password=app")
        .set("driver", "com.pivotal.gemfirexd.jdbc.EmbeddedDriver")
        .set("spark.sql.unsafe.enabled", "false")
    )) {



}

object SnappySQLContext extends LocalSQLContext
