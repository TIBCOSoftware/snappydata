package io.snappydata.examples

import org.apache.spark.sql.{SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.snappy._

/**
 *  This Spark app connects to a Snappy cluster to fetch the tables
 *  and then query the sample table and exact table and compare their results.
 */
object AirlineDataSparkApp {

  def main(args:Array[String])  {

    val conf = new SparkConf().
        setAppName("Airline Data Application")

    val sc = new SparkContext(conf)
    val snc = SnappyContext(sc)
    val colTableName = "airline"
    val rowTableName = "airlineref"
    val sampleTableName = "airlineSampled_app"

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineCodeDF: DataFrame = snc.table(rowTableName)


    // Sample the airline table to get a sampled table
    // TODO: This code will change after DataSource API is provided for sampling.
    val sampleDF = airlineDF.stratifiedSample(Map("qcs" -> "UniqueCarrier", "fraction" -> 0.01))
    sampleDF.write.mode(SaveMode.Append).format("column").
        options(Map[String, String]()).saveAsTable(sampleTableName)

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from exact table
    val start = System.currentTimeMillis
    val actualResult = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineDF("Year_"), airlineDF("Month_"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    val totalTime = (System.currentTimeMillis - start)

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from sampled table
    // TODO: Fix it after the SNAP-304 is fixed
    /*
    val startSample = System.currentTimeMillis
    val sampleResult = sampleDF.join(airlineCodeDF, sampleDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(sampleDF("UniqueCarrier"),
      sampleDF("Year_"), sampleDF("Month_"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    val totalTimeSample = (System.currentTimeMillis - startSample)
    */
    println(s"Exact table: ARR_DELAY. Query time:${totalTime}ms" );
    actualResult.show
    /* println(s"Sample table: ARR_DELAY. Query time:${totalTimeSample}ms" );
    sampleResult.show */
  }
}
