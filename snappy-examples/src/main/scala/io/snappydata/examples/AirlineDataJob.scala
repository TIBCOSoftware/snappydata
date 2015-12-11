package io.snappydata.examples

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SnappySQLJob}
import spark.jobserver.{SparkJobValid, SparkJobValidation}


/**
  * Created by swati on 2/12/15.
  */
object AirlineDataJob extends SnappySQLJob {

  override def runJob(snJobContext: C, jobConfig: Config): Any = {

    val colTableName = "airline"
    val rowTableName = "airlineref"

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snJobContext.snc.table(colTableName)

    // Row table
    val airlineCodeDF : DataFrame = snJobContext.snc.table(rowTableName)


    // Schema for column table
    val result1 = airlineDF.schema

    // Column table entry count
    val result2 = airlineDF.count

    // Schema for Row table
    val result3 = airlineCodeDF.schema

    // Row table entry count
    val result4 = airlineCodeDF.count

    // Find Flights to SAN that has been delayed(Arrival/Dep)  5 or more times in a row
    val result5 = airlineDF.select("FlightNum", "ArrDelay").where(airlineDF("Dest")==="SAN").
      groupBy("ArrDelay").count

    Map("Airline table schema" -> result1,
      "Airline table count" -> result2,
      "AirlineRef table schema" -> result3,
      "AirlineRef count" -> result4,
      "Flights to SAN that got delayed 5 or more times in a row" -> result5)

  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}