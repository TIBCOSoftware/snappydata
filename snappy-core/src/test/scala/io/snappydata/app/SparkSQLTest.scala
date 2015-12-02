package io.snappydata.app

import org.apache.spark.sql._
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources.SpecificationNotMeetException
import org.apache.spark.sql.types._

object ParseUtils extends java.io.Serializable {

  def parseInt(s: String, offset: Int, endOffset: Int): Int = {
    // Check for a sign.
    var num = 0
    var sign = -1
    val ch = s(offset)
    if (ch == '-') {
      sign = 1
    } else {
      num = '0' - ch
    }

    // Build the number.
    var i = offset + 1
    while (i < endOffset) {
      num *= 10
      num -= (s(i) - '0')
      i += 1
    }
    sign * num
  }

  def parseColumn(s: String, offset: Int, endOffset: Int,
      isInteger: Boolean): Any = {
    if (isInteger) {
      if (endOffset != (offset + 2) || s(offset) != 'N' || s(offset + 1) != 'A') {
        parseInt(s, offset, endOffset)
      } else {
        null
      }
    } else {
      s.substring(offset, endOffset)
    }
  }

  def parseRow(s: String, split: Char,
      columnTypes: Array[Boolean],
      row: ReusableRow): Unit = {
    var ai = 0
    var splitStart = 0
    val len = s.length
    var i = 0
    while (i < len) {
      if (s(i) == split) {
        row(ai) = parseColumn(s, splitStart, i, columnTypes(ai))
        ai += 1
        i += 1
        splitStart = i
      } else {
        i += 1
      }
    }
    // append remaining string
    row(ai) = parseColumn(s, splitStart, len, columnTypes(ai))
  }
}

object SparkSQLTest extends App {

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

  val conf = new org.apache.spark.SparkConf().setAppName("SparkSQLTest")
      .set("spark.logConf", "true")
      .set("spark.sql.unsafe.enabled", "false")
  //.set("spark.shuffle.sort.serializeMapOutputs", "true")
  //.set("spark.executor.memory", "1g")
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

  /*
  val ddlStr = "(Year INT," +      // NOT NULL
    "Month INT," +                 // NOT NULL
    "DayOfMonth INT," +            // NOT NULL
    "DayOfWeek INT," +             // NOT NULL
    "DepTime INT," +
    "CRSDepTime INT," +
    "ArrTime INT," +
    "CRSArrTime INT," +
    "UniqueCarrier VARCHAR(20)," + // NOT NULL
    "FlightNum INT," +
    "TailNum VARCHAR(20)," +
    "ActualElapsedTime INT," +
    "CRSElapsedTime INT," +
    "AirTime INT," +
    "ArrDelay INT," +
    "DepDelay INT," +
    "Origin VARCHAR(20)," +
    "Dest VARCHAR(20)," +
    "Distance INT," +
    "TaxiIn INT," +
    "TaxiOut INT," +
    "Cancelled INT," +
    "CancellationCode VARCHAR(20)," +
    "Diverted INT," +
    "CarrierDelay INT," +
    "WeatherDelay INT," +
    "NASDelay INT," +
    "SecurityDelay INT," +
    "LateAircraftDelay INT)"
  */

  var start: Long = 0
  var end: Long = 0
  var results, results_sampled: DataFrame = null
  var resultsC, results_sampledC: Array[Row] = null

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
        /*
        val rowRDD = airlineData.map(row => addArrDelaySlot(ParseUtils.parseRow(
          row, ',', columnTypes), arrDelayIndex, arrDelaySlotIndex))
        val rowRDD = airlineData.map(_.split(",")).map(p =>
          Row.fromSeq((0 until p.length) map { i =>
            val v = p(i)
            if (columnTypes(i)) {
              if (v != "NA") java.lang.Integer.parseInt(v) else null
            } else v
          }))
        */
        snContext.createDataFrame(rowRDD, schema)
      }
    airlineDataFrame.registerTempTable("airline")
    /*
    snContext.registerSampleTable("airline_sampled",
      airlineDataFrame.schema, Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> 0.01,
      "strataReservoirSize" -> 50))
    airlineDataFrame.insertIntoSampleTables("airline_sampled")
    */

    val airlineSampled = airlineDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> 0.01,
      "strataReservoirSize" -> 50))
    airlineSampled.registerTempTable("airline_sampled")
    /*
    val airlineSampled = airlineDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "errorLimitColumn" -> "ArrDelay",
      "errorLimitPercent" -> "10"))
    airlineSampled.registerTempTable("airline_sampled")
    */

    snContext.cacheTable("airline")
    snContext.cacheTable("airline_sampled")

    results = snContext.sql("SELECT count(*) FROM airline")
    results.map(t => "Count: " + t(0)).collect().foreach(println)
    results_sampled = snContext.sql(
      "SELECT count(*) as sample_count FROM airline_sampled")
    results_sampled.map(t => "Count sampled: " + t(0)).collect().foreach(println)
    results_sampled = snContext.sql("SELECT count(*) FROM airline_sampled")
    results_sampled.map(t => "Count approx: " + t(0)).collect().foreach(println)

    /*
    println("Creating TopK on ArrDelay")
    airlineDataFrame.filter("ArrDelay >= 0").createTopK(
      "airline_topKArrDelay", Map(
        "key" -> "UniqueCarrier",
        "size" -> "10",
        "frequencyCol" -> "ArrDelay",
        "confidence" -> "0.95",
        "eps" -> "0.01"))
        */

    //sqlContext.sql("CREATE TABLE airline " + ddlStr + " STORED AS ORC")
    //sqlContext.sql("INSERT INTO TABLE airline select * from airline1")
  }

  // show the actual topK and from topK CMS
  /*
  results = snContext.sql(
    """SELECT UniqueCarrier, SUM(ArrDelay) as SArrDelay FROM airline
       WHERE ArrDelay >= 0 GROUP BY UniqueCarrier
       ORDER BY SArrDelay DESC LIMIT 10""")
  println("=============== EXACT RESULTS ===============")
  resultsC = results.collect()
  resultsC.foreach(println)

  results = snContext.queryTopK("airline_topKArrDelay")
  println("=============== TOPK RESULTS ===============")
  resultsC = results.collect()
  resultsC.foreach(println)

  Thread.sleep(3000)
  */

  // calculate the error between full and sampled results
  results = snContext.sql( """SELECT AVG(ArrDelay), count(*),
        UniqueCarrier, Year, Month FROM airline GROUP BY
        UniqueCarrier, Year, Month ORDER BY UniqueCarrier, Year, Month""")
  println("=============== EXACT RESULTS ===============")
  resultsC = results.collect()
  resultsC.foreach(println)
  results_sampled = snContext.sql( """SELECT AVG(ArrDelay),
        ERROR ESTIMATE AVG(ArrDelay),ERROR ESTIMATE SUM(ArrDelay),
        count(*) as sample_count,
        UniqueCarrier, Year, Month FROM airline_sampled GROUP BY
        UniqueCarrier, Year, Month ORDER BY UniqueCarrier, Year, Month""")
  println()
  println("=============== APPROX RESULTS ===============")
  results_sampledC = results_sampled.collect()
  results_sampledC.foreach(println)

  /*-----------------------------------*/
  val results_sampled_table = snContext.sql(
    """SELECT SUM(ArrDelay),
        UniqueCarrier, Year, Month FROM airline GROUP BY
        UniqueCarrier, Year, Month ERRORPERCENT 15""")
  println()
  println("=============== APPROX RESULTS WITH ERROR PERCENT ===============")
  try {
    //try/catch to proceed test in case of error exception
    val results_sampledC_table = results_sampled_table.collect()
    results_sampledC_table.foreach(println)
  }
  catch {
    case ex : RuntimeException => println("Expected.."+ ex.getMessage)
  }
    /*------------------------------------*/

  // error estimates and precise errors comparing against exact data queries
  val airlineSampled = snContext.table("airline_sampled")
  val errorEstimates = airlineSampled.errorEstimateAverage("ArrDelay", 0.75)
  // Year, Month, UniqueCarrier in the order of table declaration
  val projCols = Array(3, 4, 2)
  val schema = airlineSampled.schema
  var totalErrEst1, totalErrEst2, totalVal = 0.0
  println("\n\tCOLUMN\t|\tBASE ERROR ESTIMATE\t|  ERROR ESTIMATE(75%)  |" +
      "  ACTUAL ERROR  |  ERROR PERCENT")
  compareResults(resultsC, results_sampledC, (r1, r2) => {
    if (r1.getInt(3) == r2.getInt(5) && r1.getInt(4) == r2.getInt(6) &&
        r1(2) == r2(4)) {
      val delay1 = r1.getDouble(0)
      val delay2 = r2.getDouble(0)
      val error = math.abs(delay2 - delay1)
      val errorPercent = math.abs(delay2 / delay1 - 1.0) * 100.0
      val projR = collection.Utils.projectColumns(r1, projCols, schema,
        convertToScalaRow = true)
      val errorEstimate = errorEstimates(projR)
      val spacing = if (r1.getInt(4) > 9) "\t" else "\t\t"
      if (totalVal == 0.0) {
        println()
      }
      println(projR.toString() + spacing + errorEstimate._2 + '\t' +
          errorEstimate._3 + '\t' + error + '\t' + errorPercent)
      totalErrEst1 += errorEstimate._2.abs
      totalErrEst2 += errorEstimate._3.abs
      totalVal += delay1.abs
      (error, delay1.abs)
    } else (-1.0, -1.0)
  })
  println("Total error estimate1(base) averaged (in percentage): " +
      ((totalErrEst1 * 100.0) / totalVal))
  println("Total error estimate2(75% confidence) averaged (in percentage): " +
      ((totalErrEst2 * 100.0) / totalVal))

  Thread.sleep(3000)

  for (i <- 0 until 3) {

    start = System.currentTimeMillis
    results = snContext.sql("SELECT count(*) FROM airline")
    results.map(t => "Count: " + t(0)).collect().foreach(println)
    end = System.currentTimeMillis
    println("Time taken for count(*): " + (end - start) + "ms")

    start = System.currentTimeMillis
    results = snContext.sql("SELECT count(DepTime) FROM airline")
    results.map(t => "Count: " + t(0)).collect().foreach(println)
    end = System.currentTimeMillis
    println("Time taken for count(DepTime): " + (end - start) + "ms")

    start = System.currentTimeMillis
    results = snContext.sql( """SELECT AVG(ArrDelay), count(*), UniqueCarrier,
        Year, Month FROM airline GROUP BY UniqueCarrier, Year, Month""")
    results.collect().foreach(println)
    end = System.currentTimeMillis
    println("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

    Thread.sleep(3000)

    start = System.currentTimeMillis
    results = snContext.sql( """SELECT AVG(ArrDelay), UniqueCarrier, Year,
        Month FROM airline GROUP BY UniqueCarrier, Year, Month ORDER BY
        UniqueCarrier, Year, Month""")
    //results.explain(true)
    results.collect().foreach(println)
    end = System.currentTimeMillis
    println("Time taken for AVG on GROUP BY with ORDER BY: " +
        (end - start) + "ms")

    Thread.sleep(3000)

    start = System.currentTimeMillis
    var worst1 = 0.0
    var worst2 = 0.0
    var worst1Carrier: String = null
    var worst2Carrier: String = null
    var currYear = 0
    var currMon = 0
    results = snContext.sql( """SELECT AVG(ArrDelay), UniqueCarrier, Year,
        Month FROM airline GROUP BY UniqueCarrier, Year, Month
        ORDER BY Year, Month""")
    results.collect().foreach(t => {
      val avgDelay = t.getDouble(0)
      val year = t.getInt(2)
      val mon = t.getInt(3)
      if (mon != currMon || year != currYear) {
        if (currYear != 0) {
          println(currYear + "-" + currMon + " Carrier1: " + worst1Carrier +
              "=" + worst1 + ", Carrier2: " + worst2Carrier + "=" + worst2)
        }
        currYear = year
        currMon = mon
        worst1 = 0.0
        worst2 = 0.0
        worst1Carrier = null
        worst2Carrier = null
      } else if (avgDelay > worst1) {
        worst1 = avgDelay
        worst1Carrier = t.getString(1)
      } else if (avgDelay > worst2) {
        worst2 = avgDelay
        worst2Carrier = t.getString(1)
      }
    })
    if (currYear != 0) {
      println(currYear + "-" + currMon + " Carrier1: " + worst1Carrier +
          "=" + worst1 + ", Carrier2: " + worst2Carrier + "=" + worst2)
    }
    end = System.currentTimeMillis
    println("Time taken for worst carrier processing: " + (end - start) + "ms")

    Thread.sleep(3000)
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

  def compareResults(resultsC: Array[Row], resultsTgtC: Array[Row],
      cmpRows: (Row, Row) => (Double, Double)): Unit = {

    var tgtIndex = 0
    var totalDiff = 0.0
    var totalVal = 0.0
    resultsC.foreach(row => {
      // if there is no matching row then "cmp" will return -ve value in which
      // case we go to next row from source but not in target assuming source
      // is always a superset of target (+ve value indicates diff percentage)
      val diffs = cmpRows(row, resultsTgtC(tgtIndex))
      if (diffs._1 >= 0.0) {
        totalDiff += diffs._1
        totalVal += diffs._2
        tgtIndex += 1
      }
    })
    val avgDiffPercent = (totalDiff * 100.0) / totalVal
    val missingRows = resultsC.length - tgtIndex
    println("Total difference averaged (in percentage): " + avgDiffPercent)
    println("Missing rows = " + missingRows + " from total of " + resultsC.length)
  }
}

// end of class
