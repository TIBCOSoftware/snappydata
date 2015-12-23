package io.snappydata.examples

import org.apache.spark.sql.{SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

/**
  * This application depicts how a Spark cluster can
  * connect to a Snappy cluster to fetch and query the tables
  * using Scala APIs in a Spark App.
  */
object AirlineDataSparkApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().
      setAppName("Airline Data Application")

    val sc = new SparkContext(conf)
    val snc = SnappyContext(sc)
    val colTableName = "airline"
    val rowTableName = "airlineref"

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineCodeDF: DataFrame = snc.table(rowTableName)

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from column table
    val start = System.currentTimeMillis
    val colResult = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
      equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineDF("Year_"), airlineDF("Month_"), airlineCodeDF("DESCRIPTION")).
      agg("ArrDelay" -> "avg", "FlightNum" -> "count")

    println("ARR_DELAY Result:")
    colResult.show
    val totalTimeCol = (System.currentTimeMillis - start)
    println(s"Query time:${totalTimeCol}ms\n")

    // Update the row table
    // Suppose a particular Airline company say 'Alaska Airlines Inc.'
    // re-brands itself as 'Alaska Inc.'
    val startUpd = System.currentTimeMillis
    val updateVal = udf { (DESCRIPTION: String) =>
      if (DESCRIPTION == "Alaska Airlines Inc.") "Alaska Inc." else DESCRIPTION
    }
    val updateResult = airlineCodeDF.withColumn("DESCRIPTION",
      updateVal(airlineCodeDF("DESCRIPTION")))
    println("Updated values:")
    updateResult.show
    val totalTimeUpd = (System.currentTimeMillis - startUpd)
    println(s" Query time:${totalTimeUpd}ms\n")

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from column table
    // Result of this query qill show
    val startColUpd = System.currentTimeMillis
    val colResultAftUpd = airlineDF.join(updateResult, airlineDF.col("UniqueCarrier").
      equalTo(updateResult("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineDF("Year_"), airlineDF("Month_"), updateResult("DESCRIPTION")).
      agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    println("ARR_DELAY after Updated values:")
    colResultAftUpd.show
    val totalTimeColUpd = (System.currentTimeMillis - startColUpd)
    println(s" Query time:${totalTimeColUpd}ms")
  }

}
