package io.snappydata.app

import scala.actors.Futures._

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.snappy._

/**
 * Hack to test : column, row tables using SQL, Data source API,
 * Join Column with row, concurrency, speed
 * Created by jramnara on 10/28/15.
 */
class ColumnRowSamplePerfSuite
    extends SnappyFunSuite
    with BeforeAndAfterAll {

  // context creation is handled by App main
  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    val snc = ColumnRowSamplePerfSuite.snContext
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
  }

  test("Some performance tests for column store with airline schema") {
    ColumnRowSamplePerfSuite.main(Array[String]())
  }
}

object ColumnRowSamplePerfSuite extends App {

  //var hfile: String = "/Users/jramnara/Downloads/2007-8.01.csv.parts"
  // Too large ... You need to download from GoogleDrive/snappyDocuments/data; or, use the one below
  var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile = getClass.getResource("/airlineCode_Lookup.csv").getPath
  // also available in GoogleDrive/snappyDocuments/data

  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"
  var tableName: String = "airline"

  if (args.length > 0) option(args.toList)

  val conf = new org.apache.spark.SparkConf()
      .setAppName("ColumnRowSamplePerfTest")
      .set("spark.logConf", "true")
      .set("spark.scheduler.mode", "FAIR")

  if (setMaster != null) {
    conf.setMaster(setMaster)
  }

  if (setJars != null) {
    conf.setJars(Seq(setJars))
  }

  if (executorExtraClassPath != null) {
    conf.set("spark.executor.extraClassPath", executorExtraClassPath)
  }

  if (executorExtraJavaOptions != null) {
    conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions)
  }

  var start: Long = 0
  var end: Long = 0
  var results: DataFrame = null
  var resultsC: Array[Row] = null

  val sc = new org.apache.spark.SparkContext(conf)
  val snContext = org.apache.spark.sql.SnappyContext(sc)
  snContext.sql("set spark.sql.shuffle.partitions=6")

  var airlineDataFrame: DataFrame = null

  createTableLoadData()

  // run several queries concurrently in threads ...
  val tasks = for (i <- 1 to 5) yield future {
    println("Executing task " + i)
    Thread.sleep(1000L)
    runQueries(snContext)
  }

  // wait a lot
  awaitAll(20000000L, tasks: _*)

  def createTableLoadData() = {

    if (loadData) {
      // All these properties will default when using snappyContext in the release
      val props = Map[String, String]()
      /*
      val props = Map(
        "url" -> "jdbc:snappydata:;mcast-port=0;",
        //"poolImpl" -> "tomcat",   // DOESN'T WORK?
        //"poolProps" -> "",
        //"single-hop-enabled" -> "true",
        //"driver" -> "com.pivotal.gemfirexd.jdbc.ClientDriver",
        "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
        "user" -> "app",
        "password" -> "app"
        //"persistent" -> "SYNCHRONOUS"  // THIS DOESN'T WORK .. SHOULD
      )
      */

      if (hfile.endsWith(".parquet")) {
        airlineDataFrame = snContext.read.load(hfile)
      } else {
        // Create the Airline columnar table
        airlineDataFrame = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(hfile)
      }

      println(s"The $tableName table schema ..")
      airlineDataFrame.schema.printTreeString()
      // airlineDataFrame.show(10) // expensive extract of all partitions?

      snContext.dropExternalTable(tableName, ifExists = true)

      snContext.createExternalTable(tableName, "column",
        airlineDataFrame.schema, props)
      airlineDataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)

      //Now create the airline code row replicated table
      val codeTabledf = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(codetableFile)
      println("The airline code table schema ..")
      codeTabledf.schema.printTreeString()
      codeTabledf.show(10)

      snContext.dropExternalTable("airlineCode", ifExists = true)
      /*
      val options = "OPTIONS (url 'jdbc:gemfirexd:;mcast-port=0'," +
          "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
          "poolImpl 'tomcat', " +
          "user 'app', " +
          "password 'app' ) "
      snContext.sql("create table airlineCode (code varchar, " +
          "description varchar) using row " + options)
      */
      codeTabledf.write.format("row").options(props).saveAsTable("airlineCode")

      // finally creates some samples
      val samples = airlineDataFrame.stratifiedSample(Map(
        "qcs" -> "UniqueCarrier,Year,Month", "fraction" -> 0.03,
        "strataReservoirSize" -> "50"))
      snContext.dropExternalTable("airlineSampled", ifExists = true)
      samples.write.format("column").options(props).saveAsTable("airlineSampled")

      // This will do the real work. Load the data.
      var start = System.currentTimeMillis
      results = snContext.sql(s"SELECT count(*) FROM $tableName")
      results.map(t => "Count: " + t(0)).collect().foreach(println)
      var end = System.currentTimeMillis
      msg(s"Time to load into table $tableName and count = " +
          (end - start) + " ms")

      start = System.currentTimeMillis
      results = snContext.sql(s"SELECT count(*) FROM airlineSampled")
      results.map(t => "Count: " + t(0)).collect().foreach(println)
      end = System.currentTimeMillis
      msg(s"Time to load into table airlineSampled and count = " +
          (end - start) + " ms")
    }
  }

  def runQueries(sqlContext: SQLContext): Unit = {
    var start: Long = 0
    var end: Long = 0
    var results: DataFrame = null

    for (i <- 0 to 1) {

      start = System.currentTimeMillis
      results = sqlContext.sql(s"SELECT count(*) FROM $tableName")
      results.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(*): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = sqlContext.sql(s"SELECT count(DepTime) FROM $tableName")
      results.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(DepTime): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = sqlContext.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
	        Year, Month FROM $tableName GROUP BY UniqueCarrier, Year, Month""")
      results.collect() //.foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = sqlContext.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier, t2.DESCRIPTION,
	        Year, Month FROM $tableName t1, airlineCode t2 where t1.UniqueCarrier = t2.CODE
	        GROUP BY UniqueCarrier, DESCRIPTION, Year,Month""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with JOIN + GROUP BY: " + (end - start) + "ms")

      Thread.sleep(3000)

      start = System.currentTimeMillis
      results = sqlContext.sql(
        s"""SELECT AVG(ArrDelay), UniqueCarrier, Year,
	        Month FROM $tableName GROUP BY UniqueCarrier, Year, Month ORDER BY
	        UniqueCarrier, Year, Month""")
      //results.explain(true)
      results.collect() //.foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for AVG on GROUP BY with ORDER BY: " +
          (end - start) + "ms")

      Thread.sleep(3000)

      start = System.currentTimeMillis
      results = sqlContext.sql(
        s"""SELECT AVG(ArrDelay), UniqueCarrier, Year,
	        Month FROM $tableName GROUP BY UniqueCarrier, Year, Month
	        ORDER BY Year, Month""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for worst carrier processing: " + (end - start) + "ms")

      Thread.sleep(3000)
    }
  }

  def msg(s: String) = {
    println("Thread :: " + Thread.currentThread().getName + " :: " + s)
  }

  def option(list: List[String]): Boolean = {
    list match {
      case "-hfile" :: value :: tail =>
        hfile = value
        print(" hfile " + hfile)
        option(tail)
      case "-noload" :: tail =>
        loadData = false
        print(" loadData " + loadData)
        option(tail)
      case "-set-master" :: value :: tail =>
        setMaster = value
        print(" setMaster " + setMaster)
        option(tail)
      case "-nomaster" :: tail =>
        setMaster = null
        print(" setMaster " + setMaster)
        option(tail)
      case "-set-jars" :: value :: tail =>
        setJars = value
        print(" setJars " + setJars)
        option(tail)
      case "-executor-extraClassPath" :: value :: tail =>
        executorExtraClassPath = value
        print(" executor-extraClassPath " + executorExtraClassPath)
        option(tail)
      case "-executor-extraJavaOptions" :: value :: tail =>
        executorExtraJavaOptions = value
        print(" executor-extraJavaOptions " + executorExtraJavaOptions)
        option(tail)
      case "-debug" :: tail =>
        debug = true
        print(" debug " + debug)
        option(tail)
      case opt :: tail =>
        println(" Unknown option " + opt)
        sys.exit(1)
      case Nil => true
    }
  } // end of option
}
