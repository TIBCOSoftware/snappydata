package io.snappydata.app

import java.sql.DriverManager

import org.apache.spark.sql._
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._

object ExternalStoreTest extends App {

  def addArrDelaySlot(row: ReusableRow, arrDelayIndex: Int,
      arrDelaySlotIndex: Int): Row = {
    val arrDelay =
      if (!row.isNullAt(arrDelayIndex)) row.getInt(arrDelayIndex) else 0
    row.setInt(arrDelaySlotIndex, math.abs(arrDelay) / 10)
    row
  }

  var hfile: String = getClass.getResource("/2015.parquet").getPath
  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null
  var executorExtraJavaOptions: String = null
  var setMaster: String = "local[6]"

  if (args.length > 0) {
    option(args.toList)
  }

  //val conn = DriverManager.getConnection("jdbc:snappydata://10.112.204.101:2000")
  //println(conn)
  val conf = new org.apache.spark.SparkConf().setAppName("ExternalStoreTest")
      .set("spark.logConf", "true")
  //.set("spark.shuffle.sort.serializeMapOutputs", "true")
  //.set("spark.executor.memory", "1g")
  //.set("spark.driver.memory", "1g")
  //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //.set("spark.shuffle.spill", "false")
  //.set("spark.shuffle.sort.bypassMergeThreshold", "-1")
  //.set("spark.sql.codegen", "true")
  //.set("spark.eventLog.enabled", "true")
  //.set("spark.eventLog.dir","file:/tmp/spark-events")
  //.set("spark.metrics.conf", "/home/sumedh/Projects/data/metrics.properties")
  if (setMaster != null) {
    //"local-cluster[3,2,1024]"
    conf.setMaster(setMaster)
  }

  if (setJars != null) {
    conf.setJars(Seq(setJars))
  }

  if (executorExtraClassPath != null) {
    // Intellij compile output path e.g. /wrk/out-snappy/production/MyTests/
    //conf.setExecutorEnv(Seq(("extraClassPath", executorExtraClassPath)))
    conf.set("spark.executor.extraClassPath", executorExtraClassPath)
  }

  if (debug) {
    val agentOpts = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    if (executorExtraJavaOptions != null) {
      executorExtraJavaOptions = executorExtraJavaOptions + " " + agentOpts
    } else {
      executorExtraJavaOptions = agentOpts
    }
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
        val arrDelayIndex = schemaArr.indexOf("ArrDelay")
        val arrDelaySlotIndex = schemaArr.indexOf("ArrDelaySlot")
        val rowRDD = airlineData.mapPartitions { iter =>
          val row = new ReusableRow(schemaTypes)
          iter.map { s =>
            ParseUtils.parseRow(s, ',', columnTypes, row)
            addArrDelaySlot(row, arrDelayIndex, arrDelaySlotIndex)
          }
        }
        snContext.createDataFrame(rowRDD, schema)
      }
    val props = Map(
      //"url" -> "jdbc:snappydata:;mcast-port=45672;persist-dd=false;",
      "url" -> "jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;",
      "poolImpl" -> "tomcat",
      //"single-hop-enabled" -> "true",
      //"poolProps" -> "",
      //"driver" -> "com.pivotal.gemfirexd.jdbc.ClientDriver",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    airlineDataFrame.registerAndInsertIntoExternalStore("airline", props)

    /*
    val airlineSampled = airlineDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> 0.01,
      "strataReservoirSize" -> 50))
    airlineSampled.registerAndInsertIntoExternalStore("airline_sampled", props)
    */

    results = snContext.sql("SELECT count(*) FROM airline")
    results.map(t => "Count: " + t(0)).collect().foreach(println)
    /*
    results_sampled = snContext.sql(
      "SELECT count(*) as sample_count FROM airline_sampled")
    results_sampled.map(t => "Count sampled: " + t(0)).collect().foreach(println)
    results_sampled = snContext.sql("SELECT count(*) FROM airline_sampled")
    results_sampled.map(t => "Count approx: " + t(0)).collect().foreach(println)
    */
  }

  results = snContext.sql(
    """SELECT SUM(ArrDelay) as SArrDelay FROM airline
       WHERE ArrDelay >= 0""")
  println("=============== EXACT RESULTS ===============")
  resultsC = results.collect()
  resultsC.foreach(println)

  results = snContext.sql(
    """SELECT UniqueCarrier, SUM(ArrDelay) as SArrDelay FROM airline
       WHERE ArrDelay >= 0 GROUP BY UniqueCarrier
       ORDER BY SArrDelay DESC LIMIT 10""")
  println("=============== EXACT RESULTS ===============")
  resultsC = results.collect()
  resultsC.foreach(println)

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
