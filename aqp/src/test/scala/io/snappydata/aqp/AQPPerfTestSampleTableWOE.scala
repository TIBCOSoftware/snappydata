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
    val skipTill = jobConfig.getString("skipTill").toInt
    val queryFile: String = jobConfig.getString("queryFile")
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val props = Map[String, String]()
    val pw = new PrintWriter("AQPPerfTestSampleTableWOE.out")

    pw.println("Creating AIRLINE table")
    val df = snc.read.load("/export/dev2a/users/spillai/data/1billionAirlineData")
    snc.createTable("airline", "column", df.schema, props)
    df.write.format("column").mode(SaveMode.Append).saveAsTable("airline")
    println("Created Airline base  table")

    pw.println("Creating AIRLINE_SAMPLE table")
    val sampledf = snc.createSampleTable("airline_sample",
      Some("airline"), Map("qcs" -> "UniqueCarrier ,Year_ ,Month_",
        "fraction" -> "0.05", "strataReservoirSize" -> "25", "buckets" -> "13", "overflow" -> "false"),
      allowExisting = false)
    df.write.insertInto("airline_sample")

    pw.println("Creating SampleTable_WOE table")
    val sampleWOE_df = snc.sql("Select * from airline_sample")
    snc.createTable("sampletable_WOE", "column", sampledf.schema, Map("overflow" -> "false"))
    sampleWOE_df.write.insertInto("sampletable_WOE")

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


