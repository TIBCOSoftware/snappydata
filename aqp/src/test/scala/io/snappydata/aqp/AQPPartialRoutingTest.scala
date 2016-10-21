package io.snappydata.aqp

import java.io.PrintWriter
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SnappySQLJob, SaveMode, SnappyJobValid, SnappyJobValidation, SnappyContext}

/**
 * Created by supriya on 5/10/16.
 */
object AQPPartialRoutingTest extends SnappySQLJob {
  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val numIter = jobConfig.getString("numIter").toInt
    val skipTill = jobConfig.getString("skipTill").toInt
    val queryFile: String = jobConfig.getString("queryFile");
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)
    val hfile = jobConfig.getString("dataLocation")

    //The Schema
    val ddlStr_1995To2007 = "Year_ INTEGER NOT NULL ," +
      "Month_ INTEGER NOT NULL ," +
      "DayOfMonth INTEGER NOT NULL ," +
      "DayOfWeek INTEGER NOT NULL," +
      "UniqueCarrier VARCHAR(20) NOT NULL ," +
      "TailNum VARCHAR(20)," +
      "FlightNum INTEGER," +
      "Origin VARCHAR(20)," +
      "Dest VARCHAR(20)," +
      "CRSDepTime INTEGER," +
      "DepTime INTEGER," +
      "DepDelay INTEGER," +
      "TaxiOut INTEGER," +
      "TaxiIn INTEGER," +
      "CRSArrTime VARCHAR(20)," +
      "ArrTime VARCHAR(20)," +
      "ArrDelay INTEGER," +
      "Cancelled VARCHAR(20)," +
      "CancellationCode VARCHAR(20)," +
      "Diverted INTEGER," +
      "CRSElapsedTime INTEGER," +
      "ActualElapsedTime INTEGER," +
      "AirTime INTEGER," +
      " Distance INTEGER," +
      "CarrierDelay VARCHAR(20)," +
      "WeatherDelay VARCHAR(20)," +
      "NASDelay VARCHAR(20)," +
      "SecurityDelay VARCHAR(20)," +
      "LateAircraftDelay VARCHAR(20)," +
      "ArrDelaySlot VARCHAR(20)"

    val ddlStr = "Year_ INTEGER NOT NULL ," +
      "Month_ INTEGER NOT NULL ," +
      "DayOfMonth INTEGER NOT NULL ," +
      "DayOfWeek INTEGER NOT NULL," +
      "DepTime INTEGER," +
      "CRSDepTime INTEGER," +
      "ArrTime INTEGER," +
      "CRSArrTime INTEGER," +
      "UniqueCarrier VARCHAR(20) NOT NULL ," +
      "FlightNum INTEGER,TailNum VARCHAR(20)," +
      "ActualElapsedTime INTEGER," +
      "CRSElapsedTime INTEGER," +
      "AirTime INTEGER," +
      "ArrDelay INTEGER," +
      "DepDelay INTEGER," +
      "Origin VARCHAR(20)," +
      "Dest VARCHAR(20)," +
      "Distance INTEGER," +
      "TaxiIn INTEGER," +
      "TaxiOut INTEGER," +
      "Cancelled INTEGER," +
      "CancellationCode VARCHAR(20)," +
      "Diverted INTEGER," +
      "CarrierDelay INTEGER," +
      "WeatherDelay INTEGER," +
      "NASDelay INTEGER," +
      "SecurityDelay INTEGER," +
      "LateAircraftDelay INTEGER," +
      "ArrDelaySlot INTEGER"

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPartialRoutingTest.out")
    val airlineDataFrame: DataFrame = snc.read.load(hfile)

    //Create an empty table with VARCHAR datatype instead of String
    snc.sql("DROP TABLE IF EXISTS AIRLINE")
    snc.sql( s"""CREATE TABLE AIRLINE ($ddlStr_1995To2007) PARTITION BY COLUMN (UNIQUECARRIER) BUCKETS 11""")

    //Populate the AIRLINE table as row table
    airlineDataFrame.write.format("row").mode(SaveMode.Append).saveAsTable("AIRLINE")

    //Create an index on column 'UNIQUECARRIER'
    snc.sql("CREATE INDEX UNIQUECARRIER_INDEX on AIRLINE(UNIQUECARRIER)")

    Try {
      AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/AQPPartialRoutingTest.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }
  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
