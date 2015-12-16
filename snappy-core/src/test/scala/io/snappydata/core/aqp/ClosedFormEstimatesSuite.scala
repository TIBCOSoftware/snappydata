package io.snappydata.core

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.collection.ReusableRow
import org.apache.spark.sql._
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._


/**
 * Created by sbhokare on 16/12/15.
 */
class ClosedFormEstimatesSuite extends FunSuite with BeforeAndAfter {

  var hfile: String = "./tests-common/src/main/resources/2015.parquet"
  var loadData: Boolean = true
  var setMaster: String = "local[6]"


  val conf = new org.apache.spark.SparkConf().setAppName("SparkSQLTest")
      .set("spark.logConf", "true")
      .set("spark.sql.unsafe.enabled", "false")

  if (setMaster != null) {
    //"local-cluster[3,2,1024]"
    conf.setMaster(setMaster)
  }

  def addArrDelaySlot(row: ReusableRow, arrDelayIndex: Int,
      arrDelaySlotIndex: Int): Row = {
    val arrDelay =
      if (!row.isNullAt(arrDelayIndex)) row.getInt(arrDelayIndex) else 0
    row.setInt(arrDelaySlotIndex, math.abs(arrDelay) / 10)
    row
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
            io.snappydata.app.ParseUtils.parseRow(s, ',', columnTypes, row)
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


    val airlineSampled = airlineDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,Year,Month",
      "fraction" -> 0.01,
      "strataReservoirSize" -> 50))
    airlineSampled.registerTempTable("airline_sampled")

    snContext.cacheTable("airline")
    snContext.cacheTable("airline_sampled")

  }

  test("lower bound and upper bound with closed form") {
    val results_sampled_table = snContext.sql(
      """SELECT SUM(ArrDelay), LOWER BOUND SUM(ArrDelay), UPPER BOUND SUM(ArrDelay),
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier ERRORPERCENT 12""")
    println()
    println("=============== APPROX RESULTS WITH ERROR PERCENT ===============")
    try {
      //try/catch to proceed test in case of error exception
      val results_sampledC_table = results_sampled_table.collect()
      results_sampledC_table.foreach(println)
    }
    catch {
      case rte: RuntimeException => println(rte)
    }
  }
}