package io.snappydata.examples

import java.io.{PrintWriter}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SnappySQLJob}
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Fetches already created tables. Airline table is already persisted in
 * Snappy store. Cache the airline table in Spark cache as well for
 * comparison. Sample airline table and persist it in Snappy store.
 * Run a aggregate query on all the three tables and return the results in
 * a Map.This Map will be sent over REST.
 */
object AirlineDataJob extends SnappySQLJob {

  override def runJob(snc: C, jobConfig: Config): Any = {
    val colTableName = "airline"
    val parquetTable = "STAGING_AIRLINE"
    val rowTableName = "airlineref"
    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val pw = new PrintWriter("resultOutPut.out")

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineCodeDF: DataFrame = snc.table(rowTableName)
    val airlineParquetDF: DataFrame = snc.table(parquetTable)

    // Cache the airline data in a Spark table as well
    airlineParquetDF.cache()
    airlineParquetDF.count()

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from column table
   val actualResult = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineDF("Year_"), airlineDF("Month_"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    val start = System.currentTimeMillis
    val result = actualResult.collect()
    val totalTime = (System.currentTimeMillis - start)
    pw.println(s"****** Output of Airline Snappy table took ${totalTime}ms ******")
    result.foreach(rs => {
      pw.println(rs.toString)
    })

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from column table
    val parquetResult = airlineParquetDF.join(airlineCodeDF, airlineParquetDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineParquetDF("UniqueCarrier"),
      airlineParquetDF("Year"), airlineParquetDF("Month"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    val startP = System.currentTimeMillis
    val resultP = parquetResult.collect()
    val totalTimeP = (System.currentTimeMillis - startP)
    pw.println(s"\n****** Output of Airline Spark table took ${totalTimeP}ms******")
    resultP.foreach(rs => {
      pw.println(rs.toString)
    })

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from column table
    // TODO: Fix it after the SNAP-304 is fixed
    /*
    val startSample = System.currentTimeMillis
    val sampleResult = sampleDF.select("FlightNum", "ArrDelay").
        filter("FlightNum = 2626").groupBy(sampleDF("FlightNum")).agg("ArrDelay" -> "count")
    val totalTimeSample = (System.currentTimeMillis - startSample)
    */

    pw.close()
    Map("The output of the queries is in the following file: " -> s"${getCurrentDirectory}/resultOutPut.out")
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
