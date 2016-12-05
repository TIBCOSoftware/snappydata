package io.snappydata.aqp

import java.io.PrintWriter
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql._


/** This test will let us know the cost of sampling
  * Created by supriya on 4/10/16.
  */
object AQPPerfTestSampleTableWOE extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val numIter = jobConfig.getString("numIter").toInt
    val dataLocation = jobConfig.getString("dataLocation")
    val skipTill = jobConfig.getString("skipTill").toInt
    val queryFile: String = jobConfig.getString("queryFile")
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPerfTestSampleTableWOE.out")

    pw.println("Creating AIRLINE table")
    snc.sql(s"CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet OPTIONS(path '$dataLocation')")
    val df = snc.sql(s"CREATE TABLE AIRLINE USING column OPTIONS(buckets '11') AS ( " +
      " SELECT Year AS Year_, Month AS Month_ , DayOfMonth," +
      " DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime," +
      " UniqueCarrier, FlightNum, TailNum, ActualElapsedTime," +
      " CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin," +
      " Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode," +
      " Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay," +
      " LateAircraftDelay, ArrDelaySlot" +
      " FROM STAGING_AIRLINE)")
   /* val df = snc.read.load(dataLocation)
    snc.createTable("airline", "column", df.schema, props)
    df.write.format("column").mode(SaveMode.Append).saveAsTable("airline")*/
    println("Created Airline base  table")

    pw.println("Creating AIRLINE_SAMPLE table")
   /* val sampledf = snc.createSampleTable("airline_sample",
      Some("airline"), Map("qcs" -> "UniqueCarrier ,Year_ ,Month_",
        "fraction" -> "0.05", "strataReservoirSize" -> "25", "buckets" -> "13", "overflow" -> "false"),
      allowExisting = false)
    df.write.insertInto("airline_sample")
*/
    val sampledf = snc.sql(s"create sample table airline_sample on airline " +
     "options(qcs 'UniqueCarrier,Year_,Month_', fraction '0.01'," +
     "  strataReservoirSize '50',overflow 'false') AS (select * from airline)")

    pw.println("Creating SampleTable_WOE table")
   /* val sampleWOE_df = snc.sql("Select * from airline_sample")
    snc.createTable("sampletable_WOE", "column", sampledf.schema, Map("overflow" -> "false"))
    sampleWOE_df.write.insertInto("sampletable_WOE")
*/
    snc.sql(s"CREATE TABLE sampletable_WOE USING column OPTIONS(overflow 'false') AS (select * from airline_sample)")

    val df1: DataFrame = snc.sql(s"select count(*) from sampletable_WOE")
    df1.collect()

    Try {
      AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/AQPPerfTestSampleTableWOE.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}


