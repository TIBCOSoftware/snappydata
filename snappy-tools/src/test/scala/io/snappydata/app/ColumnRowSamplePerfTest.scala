package io.snappydata.app

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._
import shapeless.option

import scala.actors.Futures._

/**
 * Hack
 * Created by jramnara on 10/28/15.
 */
object ColumnRowSamplePerfTest extends App{

  var hfile: String = "/Users/jramnara/Downloads/2007-8.csv"
  // Too large ... You need to download from GoogleDrive/snappyData/data; or, use the one below
  //var hfile: String = getClass.getResource("/2015.parquet").getPath
  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"
  var tablename: String = "airlineSampled"

  if (args.length > 0) option(args.toList)

  //val conn = DriverManager.getConnection("jdbc:snappydata://10.112.204.101:2000")
  //println(conn)
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

  // Set the url of the database
  // use these two when you want to test with snappydata database url
  // Alter the url property when creating the table below as well
  //conf.set("gemfirexd.db.url", "jdbc:snappydata:;mcast-port=45672;persist-dd=false;")
  //conf.set("gemfirexd.db.driver", "com.pivotal.gemfirexd.jdbc.EmbeddedDriver")

  var start: Long = 0
  var end: Long = 0
  var results: DataFrame = null
  var resultsC: Array[Row] = null

  val sc = new org.apache.spark.SparkContext(conf)
  val snContext = org.apache.spark.sql.SnappyContext(sc)
  snContext.sql("set spark.sql.shuffle.partitions=6")

  if (loadData) {
    val airlineDataFrame: DataFrame =
      if (hfile.endsWith(".parquet")) {
        snContext.read.load(hfile)
      } else {
        val airlineData = sc.textFile(hfile)

        val schemaString = "Year,Month,DayOfMonth,DayOfWeek,DepTime,CRSDepTime," +
          "ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime," +
          "CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn," +
          "TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay," +
          "WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,ArrDelaySlot"
        val schemaArr = schemaString.split(",")
        val schemaTypes = List(IntegerType, IntegerType, IntegerType, IntegerType,
          IntegerType, IntegerType, IntegerType, IntegerType, StringType,
          IntegerType, StringType, IntegerType, IntegerType, IntegerType,
          IntegerType, IntegerType, StringType, StringType, IntegerType,
          IntegerType, IntegerType, IntegerType, StringType, IntegerType,
          IntegerType, IntegerType, IntegerType, IntegerType, IntegerType,
          IntegerType)

        val schema = StructType(schemaArr.zipWithIndex.map {
          case (fieldName, i) => StructField(
            fieldName, schemaTypes(i), i >= 4)
        })

        val columnTypes = schemaTypes.map {
          _ == IntegerType
        }.toArray

        val rowRDD: RDD[Row] = airlineData.mapPartitions { iter =>
          val row = new ReusableRow(schemaTypes)
          iter.map { s =>
            ParseUtils.parseRow(s, ',', columnTypes, row)
//            addArrDelaySlot(row, arrDelayIndex, arrDelaySlotIndex)
            row
          }
        }
        snContext.createDataFrame(rowRDD, schema)
      }

    val props = Map(
      //"url" -> "jdbc:snappydata:;mcast-port=0;persist-dd=false;",
      "url" -> "jdbc:gemfirexd:;mcast-port=0;persist-dd=false;",
      //"poolImpl" -> "tomcat",   // DOESN'T WORK?
      //"single-hop-enabled" -> "true",
      //"poolProps" -> "",
      //"driver" -> "com.pivotal.gemfirexd.jdbc.ClientDriver",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    snContext.dropExternalTable("airline", true)
    airlineDataFrame.registerAndInsertIntoExternalStore("airline", props)

    // METHODS BELOW FAIL FOR UNKNOWN REASON
    //snContext.createExternalTable("airline", "column", airlineDataFrame.schema, props)
    //airlineDataFrame.write.format("column").mode(SaveMode.Append).options(props).insertInto(tablename)

    val samples = airlineDataFrame.stratifiedSample(Map("qcs" -> "UniqueCarrier,Year,Month", "fraction" -> 0.03,
      "strataReservoirSize" -> "50"))
    samples.registerAndInsertIntoExternalStore("airlineSampled", props)
    //samples.write.format("column").options(props).saveAsTable("airlineSampled")


    results = snContext.sql(s"SELECT count(*) FROM $tablename")
    results.map(t => "Count: " + t(0)).collect().foreach(println)
    /*
    results_sampled = snContext.sql(
      "SELECT count(*) as sample_count FROM airline_sampled")
    results_sampled.map(t => "Count sampled: " + t(0)).collect().foreach(println)
    results_sampled = snContext.sql("SELECT count(*) FROM airline_sampled")
    results_sampled.map(t => "Count approx: " + t(0)).collect().foreach(println)
    */
  }
  // run several queries concurrently in threads ...
  val tasks = for (i <- 1 to 10) yield future {
    println("Executing task " + i)
    Thread.sleep(1000L)
    runQueries(snContext)
  }

  val ret = awaitAll(20000000L, tasks: _*) // wait a lot



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

  def addArrDelaySlot(row: ReusableRow, arrDelayIndex: Int,
                      arrDelaySlotIndex: Int): Row = {
    val arrDelay =
      if (!row.isNullAt(arrDelayIndex)) row.getInt(arrDelayIndex) else 0
    row.setInt(arrDelaySlotIndex, math.abs(arrDelay) / 10)
    row
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
