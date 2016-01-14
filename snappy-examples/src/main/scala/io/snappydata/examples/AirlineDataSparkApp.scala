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
    snc.sql("set spark.sql.shuffle.partitions=5")

    val colTableName = "airline"
    val rowTableName = "airlineref"

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineCodeDF: DataFrame = snc.table(rowTableName)

    // Data Frame query :Which Airlines Arrive On Schedule? JOIN with reference table
    val colResult = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineCodeDF("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    println("Airline arrival schedule")
    val start = System.currentTimeMillis
    colResult.show
    val totalTimeCol = (System.currentTimeMillis - start)
    println(s"Query time:${totalTimeCol}ms\n")

    // Update the row table
    // Suppose a particular Airline company say 'Alaska Airlines Inc.'
    // re-brands itself as 'Alaska Inc.'
    val updateVal = udf { (DESCRIPTION: String) =>
      if (DESCRIPTION == "Delta Air Lines Inc.") "Delta America" else DESCRIPTION
    }
    val updateResult = airlineCodeDF.withColumn("DESCRIPTION",
      updateVal(airlineCodeDF("DESCRIPTION")))
    println("Updated values:")
    val startUpd = System.currentTimeMillis
    updateResult.show
    val totalTimeUpd = (System.currentTimeMillis - startUpd)
    println(s" Query time:${totalTimeUpd}ms\n")

    // Data Frame query :Which Airlines Arrive On Schedule? JOIN with reference table
    val colResultAftUpd = airlineDF.join(updateResult, airlineDF.col("UniqueCarrier").
        equalTo(updateResult("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      updateResult("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    println("Airline arrival schedule after Updated values:")
    val startColUpd = System.currentTimeMillis
    colResultAftUpd.show
    val totalTimeColUpd = (System.currentTimeMillis - startColUpd)
    println(s" Query time:${totalTimeColUpd}ms")
  }

}
