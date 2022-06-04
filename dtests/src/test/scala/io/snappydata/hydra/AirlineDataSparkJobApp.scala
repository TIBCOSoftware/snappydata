/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.hydra

import org.apache.spark.sql.{Row, DataFrame, SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * This application depicts how a Spark cluster can
  * connect to a Snappy cluster to fetch and query the tables
  * using Scala APIs in a Spark App.
  */

object AirlineDataSparkJobApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("Airline Data Application_" + System.currentTimeMillis()).
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    snc.sql("set spark.sql.shuffle.partitions=6")

    val colTableName = "airline"
    val rowTableName = "airlineref"

    // Get the tables that were created using sql scripts via snappy-sql
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

    // Suppose a particular Airline company say 'Delta Air Lines Inc.'
    // re-brands itself as 'Delta America'.Update the row table.
    val query: String = " CODE ='DL'"
    val newColumnValues: Row = Row("Delta America")
    snc.update(rowTableName, query, newColumnValues, "DESCRIPTION")

    // Data Frame query :Which Airlines Arrive On Schedule? JOIN with reference table
    val colResultAftUpd = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
        equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineCodeDF("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    println("Airline arrival schedule after Updated values:")
    val startColUpd = System.currentTimeMillis
    colResultAftUpd.show
    val totalTimeColUpd = (System.currentTimeMillis - startColUpd)
    println(s" Query time:${totalTimeColUpd}ms")
    // scalastyle:on println
  }

}

