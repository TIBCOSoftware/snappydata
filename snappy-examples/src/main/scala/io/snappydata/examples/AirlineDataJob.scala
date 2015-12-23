package io.snappydata.examples

import com.typesafe.config.Config
import org.apache.spark.sql.{SaveMode, DataFrame, SnappySQLJob}
import spark.jobserver.{SparkJobValid, SparkJobValidation}
import org.apache.spark.sql.snappy._

/**
 * Fetches already created tables, sample them and run a aggregate query and returns
 * the results in a Map. This Map will be sent over REST.
 */
object AirlineDataJob extends SnappySQLJob {

  override def runJob(snc: C, jobConfig: Config): Any = {

    val colTableName = "airline"
    val rowTableName = "airlineref"
    val sampleTableName = "airlineSampled"

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineCodeDF: DataFrame = snc.table(rowTableName)

    // Sample the airline table to get a sampled table
    // TODO: This code will change after DataSource API is provided for sampling.
    val sampleDF = airlineDF.stratifiedSample(Map("qcs" -> "UniqueCarrier", "fraction" -> 0.01))
    sampleDF.write.mode(SaveMode.Append).format("column").
        options(Map[String, String]()).saveAsTable(sampleTableName)

    // Data Frame query to get number of times FlightNum 2626 has been delayed
    val actualResult = airlineDF.select("FlightNum", "ArrDelay").
        filter("FlightNum = 2626").groupBy(airlineDF("FlightNum")).agg("ArrDelay" -> "count")
    val start = System.currentTimeMillis
    val result = actualResult.collect()
    val totalTime = (System.currentTimeMillis - start)
    Map(s"Exact table: ARR_DELAY :(${totalTime}ms)" -> result)
    // Data Frame query to get number of times FlightNum 2626 has been delayed
    // TODO: Fix it after the SNAP-304 is fixed
    /*
    val startSample = System.currentTimeMillis
    val sampleResult = sampleDF.select("FlightNum", "ArrDelay").
        filter("FlightNum = 2626").groupBy(sampleDF("FlightNum")).agg("ArrDelay" -> "count")
    val totalTimeSample = (System.currentTimeMillis - startSample)
    */


    /*Map(s"Exact table: ARR_DELAY :(${totalTime}ms)" -> actualResult.collect(),
      s"Sample table: ARR_DELAY :(${totalTimeSample}ms)" -> sampleResult.collect()
    )*/
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
