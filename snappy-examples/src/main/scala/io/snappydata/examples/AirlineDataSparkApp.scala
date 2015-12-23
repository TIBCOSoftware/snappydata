package io.snappydata.examples

import org.apache.spark.sql.{SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{ DataFrame}
import org.apache.spark.sql.functions._

/**
 *  This Spark app connects to a Snappy cluster to fetch the tables
 *  and then query the table
 */
object AirlineDataSparkApp {

  def main(args:Array[String])  {

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
    val colResult = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
        airlineDF("Year_"), airlineDF("Month_"), airlineCodeDF("DESCRIPTION")).
        agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    println("ARR_DELAY Result:")
    val start = System.currentTimeMillis
    colResult.show
    val totalTimeCol = (System.currentTimeMillis - start)
    println(s"Query time:${totalTimeCol}ms\n")

    // Update the row table
    // Suppose a particular Airline company say 'Alaska Airlines Inc.'
    // re-brands itself as 'Alaska Virgin America'
    val updateVal = udf {(DESCRIPTION: String) =>
      if(DESCRIPTION == "Alaska Airlines Inc.") "Alaska Virgin America" else DESCRIPTION
    }
    val updateResult = airlineCodeDF.withColumn("DESCRIPTION", updateVal(airlineCodeDF("DESCRIPTION")))
    println("Updated values:")
    val startUpd = System.currentTimeMillis
    updateResult.show
    val totalTimeUpd = (System.currentTimeMillis - startUpd)
    println(s" Query time:${totalTimeUpd}ms\n")

    // Data Frame query to get average ARR_DELAY for a carrier monthwise from column table
    val colResultAftUpd = airlineDF.join(updateResult, airlineDF.col("UniqueCarrier").
        equalTo(updateResult("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineDF("Year_"), airlineDF("Month_"), updateResult("DESCRIPTION")).
        agg("ArrDelay" -> "avg", "FlightNum" -> "count")
    println("ARR_DELAY after Updated values:")
    val startColUpd = System.currentTimeMillis
    colResultAftUpd.show
    val totalTimeColUpd = (System.currentTimeMillis - startColUpd)
    println(s" Query time:${totalTimeColUpd}ms")
  }

}
