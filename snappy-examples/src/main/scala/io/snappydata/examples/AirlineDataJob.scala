package io.snappydata.examples

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

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineParquetDF: DataFrame = snc.table(parquetTable)

    // Cache the airline data in a Spark table as well
    airlineParquetDF.cache()
    airlineParquetDF.count()

    // Query Airline table in Snappy Store to get number of times FlightNum 2626 has been delayed
    val actualResult = airlineDF.select("FlightNum", "ArrDelay").
        filter("FlightNum = 2626").groupBy(airlineDF("FlightNum")).agg("ArrDelay" -> "count")
    val start = System.currentTimeMillis
    val result = actualResult.collect()
    val totalTime = (System.currentTimeMillis - start)

    // Query Airline table in Spark Cache to get number of times FlightNum 2626 has been delayed
    val parquetResult = airlineParquetDF.select("FlightNum", "ArrDelay").
        filter("FlightNum = 2626").groupBy(airlineParquetDF("FlightNum")).agg("ArrDelay" -> "count")
    val startP = System.currentTimeMillis
    val resultP = parquetResult.collect()
    val totalTimeP = (System.currentTimeMillis - startP)

    // Data Frame query to get number of times FlightNum 2626 has been delayed
    // TODO: Fix it after the SNAP-304 is fixed
    /*
    val startSample = System.currentTimeMillis
    val sampleResult = sampleDF.select("FlightNum", "ArrDelay").
        filter("FlightNum = 2626").groupBy(sampleDF("FlightNum")).agg("ArrDelay" -> "count")
    val totalTimeSample = (System.currentTimeMillis - startSample)
    */

    Map(s"Airline Snappy table: ARR_DELAY :(Time taken: ${totalTime}ms)" -> result,
      s"Airline Spark table: ARR_DELAY :(Time taken: ${totalTimeP}ms)" ->resultP)

  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
