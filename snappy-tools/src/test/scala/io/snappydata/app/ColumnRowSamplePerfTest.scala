package io.snappydata.app

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._

import scala.actors.Futures._

/**
 * Hack to test : column, row tables using SQL, Data source API, Join Column with row, concurrency, speed
 * Created by jramnara on 10/28/15.
 */
object ColumnRowSamplePerfTest extends App{

  var hfile: String = "/Users/jramnara/Downloads/2007-8.01.csv.parts"
  // Too large ... You need to download from GoogleDrive/snappyDocuments/data; or, use the one below
  //var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile: String = "/Users/jramnara/Downloads/airlineCode_Lookup.csv"
  // also available in GoogleDrive/snappyDocuments/data

  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"
  var tablename: String = "airline"

  if (args.length > 0) option(args.toList)

  val conf = new org.apache.spark.SparkConf().setAppName("ColumnRowSamplePerfTest")
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
  val tasks = for (i <- 1 to 10) yield future {
    println("Executing task " + i)
    Thread.sleep(1000L)
    runQueries(snContext)
  }

  val ret = awaitAll(20000000L, tasks: _*) // wait a lot



  def createTableLoadData() = {

    if (loadData) {

      if (hfile.endsWith(".parquet")) {
        airlineDataFrame = snContext.read.load(hfile)
      } else {

        // All these properties will default when using snappyContext in the release
        val props = Map(
          //"url" -> "jdbc:snappydata:;mcast-port=0;persist-dd=false;",
          //"url" -> "jdbc:gemfirexd:;mcast-port=0;persist-dd=false;",
          "url" -> "jdbc:gemfirexd:;mcast-port=0;",
          //"poolImpl" -> "tomcat",   // DOESN'T WORK?
          //"single-hop-enabled" -> "true",
          //"poolProps" -> "",
          //"driver" -> "com.pivotal.gemfirexd.jdbc.ClientDriver",
          "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
          "user" -> "app",
          "password" -> "app"
          //"persistent" -> "SYNCHRONOUS"  // THIS DOESN'T WORK .. SHOULD
        )

        // Create the Airline columnar table
        val airlineDataFrame = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(hfile)
        println("The airline table schema ..")
        airlineDataFrame.schema.printTreeString()
        // airlineDataFrame.show(10) // expensive extract of all partitions?

        snContext.dropExternalTable("airline", true)
        airlineDataFrame.registerAndInsertIntoExternalStore("airline", props)

        // METHODS BELOW FAIL FOR TBD REASON .. something to do with the data?
        //snContext.createExternalTable("airline", "column", airlineDataFrame.schema, props)
        //airlineDataFrame.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("airline")


        //Now create the airline code row replicated table
        val codeTabledf = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(codetableFile)
        println("The airline code table schema ..")
        codeTabledf.schema.printTreeString()
        codeTabledf.show(10)

        snContext.dropExternalTable("airlineCode", true)
        val options = "OPTIONS (url 'jdbc:gemfirexd:;mcast-port=0;user=app;password=app;persist-dd=false' ," +
          "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
          "poolImpl 'tomcat', " +
          "user 'app', " +
          "password 'app' ) "
        //snContext.sql("create table airlineCode (code varchar, description varchar) using jdbc " + options)

        //snContext.createExternalTable("airlineCode", "ROW", df.schema, props)
        codeTabledf.write.format("jdbc").options(props).saveAsTable("airlineCode") //insertInto("airlineCode")

        // can I use registerAndInsertIntoExternalStore to create ROW table?
        //airlineDataFrame.registerAndInsertIntoExternalStore("airlineCode", props)

        // finally creates some samples
        val samples = airlineDataFrame.stratifiedSample(Map("qcs" -> "UniqueCarrier,Year,Month", "fraction" -> 0.03,
          "strataReservoirSize" -> "50"))
        samples.registerAndInsertIntoExternalStore("airlineSampled", props)
        //samples.write.format("column").options(props).saveAsTable("airlineSampled")


        // This will do the real work. Load the data.
        val start = System.currentTimeMillis
        results = snContext.sql(s"SELECT count(*) FROM $tablename")
        results.map(t => "Count: " + t(0)).collect().foreach(println)
        val end = System.currentTimeMillis
        msg(s"Time to load into table $tablename and count = " + (end - start) + " ms")

      }
    }
  }


  def runQueries(sqlContext: SQLContext ): Unit = {
    var start: Long = 0
    var end: Long = 0
    var results: DataFrame = null

    for (i <- 0 to 3) {

      start = System.currentTimeMillis
      results = sqlContext.sql(s"SELECT count(*) FROM $tablename")
      results.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(*): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = sqlContext.sql(s"SELECT count(DepTime) FROM $tablename")
      results.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(DepTime): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = sqlContext.sql(s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
	        Year, Month FROM $tablename GROUP BY UniqueCarrier, Year, Month""")
      results.collect()   //.foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = sqlContext.sql(s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier, t2.DESCRIPTION,
	        Year, Month FROM $tablename t1, airlineCode t2 where t1.UniqueCarrier = t2.CODE
	        GROUP BY UniqueCarrier, DESCRIPTION, Year,Month""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with JOIN + GROUP BY: " + (end - start) + "ms")

      Thread.sleep(3000)

      start = System.currentTimeMillis
      results = sqlContext.sql(s"""SELECT AVG(ArrDelay), UniqueCarrier, Year,
	        Month FROM $tablename GROUP BY UniqueCarrier, Year, Month ORDER BY
	        UniqueCarrier, Year, Month""")
      //results.explain(true)
      results.collect()  //.foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for AVG on GROUP BY with ORDER BY: " +
        (end - start) + "ms")

      Thread.sleep(3000)

      start = System.currentTimeMillis
      results = sqlContext.sql(s"""SELECT AVG(ArrDelay), UniqueCarrier, Year,
	        Month FROM $tablename GROUP BY UniqueCarrier, Year, Month
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

