package io.snappydata.aqp

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappyContext, SnappySQLJob}

import scala.util.{Failure, Success, Try}

/**
 * Created by supriya on 30/11/16.
 */
object ErrorEstimationValidation extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw = new PrintWriter("ErrorEstimateValidation.out")
    val hfile :String = "/data/ParquetDataFiles/airlineParquetData_2007-15"
    snc.dropTable("airline_sample", ifExists = true)
    snc.dropTable("airline", ifExists = true)
    snc.sql(s"CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet OPTIONS(path '$hfile')")
    snc.sql(s"CREATE TABLE AIRLINE USING column OPTIONS(buckets '11') AS ( " +
      " SELECT Year AS Year_, Month AS Month_ , DayOfMonth," +
      " DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime," +
      " UniqueCarrier, FlightNum, TailNum, ActualElapsedTime," +
      " CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin," +
      " Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode," +
      " Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay," +
      " LateAircraftDelay, ArrDelaySlot" +
      " FROM STAGING_AIRLINE)")

    val basecount = snc.sql("select count(*) from airline")
    val result = basecount.collect()
    result.foreach(rs => {
      pw.println("Base table count is " + rs.toString)
    })

    snc.sql(s"create sample table airline_sample on airline " +
      "options(qcs 'UniqueCarrier,Year_,Month_', fraction '0.01'," +
      "  strataReservoirSize '50') AS (select * from airline)")

    val countResult = snc.sql("select count(*) as sample_ from airline_sample")
    val result1 = countResult.collect()
    result1.foreach(rs => {
      pw.println("Sample table count is " + rs.toString)
    })

    println("Finished creating sample table")

    Try {
      val result = snc.sql("Select uniqueCarrier, sum(ArrDelay) as x ,count(*) from airline group by uniqueCarrier order by uniqueCarrier desc")
      val rowCnt = result.count()
      val uniqueCarrier_base = new Array[String](rowCnt.toInt)
      val actualVal = new Array[Long](rowCnt.toInt)
      val countVal = new Array[Long](rowCnt.toInt)
      println("The total row cnt from base table is " + rowCnt)
      val collectActualVal = result.collect()
      println("The total row  in collectActualVal is " + collectActualVal.length)
      for (i <- 0 to collectActualVal.length - 1) {
        uniqueCarrier_base(i) = collectActualVal(i).getString(0)
        actualVal(i) = collectActualVal(i).getLong(1)
        countVal(i) = collectActualVal(i).getLong(2)
        println("Supriya1 :actual value is " + actualVal(i))
      }

      val sampleVal = snc.sql("Select uniqueCarrier, sum(ArrDelay) as x , relative_error(x),count(*) as sample_ from airline group by uniqueCarrier order by uniqueCarrier desc with error 0.2 confidence 0.9999 behavior 'do_nothing'")
      val sampleRowCnt = sampleVal.count()
      val uniqueCarrier_sample = new Array[String](sampleRowCnt.toInt)
      val estimatedVal = new Array[Long](sampleRowCnt.toInt)
      val relativeVal = new Array[Double](sampleRowCnt.toInt)
      val sampleCountVal = new Array[Long](sampleRowCnt.toInt)
      println("The total row cnt from sample table is " + sampleRowCnt)
      val collectSampleVal = sampleVal.collect()
      for (i <- 0 to collectSampleVal.length - 1) {
        uniqueCarrier_sample(i) = collectSampleVal(i).getString(0)
        estimatedVal(i) = collectSampleVal(i).getLong(1)
        relativeVal(i) = collectSampleVal(i).getDouble(2)
        sampleCountVal(i) = collectSampleVal(i).getLong(3)
        println("Supriya2 : absoluteVal is " + relativeVal(i))
      }

      for (i <- 0 to actualVal.length - 1) {
        val actualDiff = java.lang.Math.abs(actualVal(i) - estimatedVal(i))
        val actualError = (actualDiff.toDouble/actualVal(i).toDouble)
        pw.println("BaseUniqueCarrier = " + uniqueCarrier_base(i) + " SampleUniqueCarrier = " + uniqueCarrier_sample(i))
        pw.println("Base Table count = " + countVal(i) + " Sample Table count " + sampleCountVal(i))
        pw.println("ActualValue is = " + actualVal(i) + " Estimated Value is =  " +
          estimatedVal(i) + " RelativeError = " + relativeVal(i) + " actualDiff = " + actualDiff + " actualError = " + actualError)
      }
    }
    match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/ErrorEstimateValidation.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }
  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}

