package io.snappydata.aqp

import java.io.PrintWriter
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappyContext, SnappySQLJob}


/** This test will let us know the cost of sampling
  * Created by supriya on 4/10/16.
  */
object AQPPerfTestSampleTableWOE extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val numIter = jobConfig.getString("numIter").toInt
    val skipTill = jobConfig.getString("skipTill").toInt
    val queryFile: String = jobConfig.getString("queryFile")
    val sampleDataLocation: String = jobConfig.getString("sampleDataLocation")
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPerfTestSampleTableWOE.out")

    pw.println("Creating AIRLINE_SAMPLE table")
    snc.sql(s"CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE1 " +
      " OPTIONS(buckets '7',qcs 'UniqueCarrier, Year_, Month_'," +
      " fraction '0.03',strataReservoirSize '50') " +
      " AS (SELECT * FROM AIRLINE1);")

    createSampleTableWOE()

    def createSampleTableWOE(): Unit = {
      storeSampleTableValue()
      println("Creating table SampleTableWOE ")
      println("sampleTableLocation is " + sampleDataLocation)
      pw.println("Creating SampleTableWOE table")

      //Use the saved sampled data to create a normal column table ,this is to avoid resampling as this data is already sampled.
      val df = snc.read.load(sampleDataLocation).toDF("YEAR_", "Month_", "DayOfMonth",
        "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum", "Origin", "Dest", "CRSDepTime", "DepTime", "DepDelay", "TaxiOut",
        "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime",
        "ActualElapsedTime", "AirTime", "Distance", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
        "LateAircraftDelay", "ArrDelaySlot", "SNAPPY_SAMPLER_WEIGHTAGE")
      snc.createTable("sampleTable_WOE", "column", df.schema, Map("buckets" -> "7"))
      df.write.insertInto("sampleTable_WOE")
      val actualResult = snc.sql("select count(*) as sample_ from sampleTable_WOE")
      val result = actualResult.collect()
      result.foreach(rs => {
        pw.println(rs.toString)
      })
    }
    //Save the already created sampletable data as a parquet
    def storeSampleTableValue(): Unit = {
      val df = snc.sql("SELECT * from airline_sample")
      df.write.parquet(sampleDataLocation)
    }

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


