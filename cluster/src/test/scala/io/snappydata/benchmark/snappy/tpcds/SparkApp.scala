/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.benchmark.snappy.tpcds

import java.io.{File, FileOutputStream, PrintStream}

import io.snappydata.benchmark.snappy.tpch.QueryExecutor

import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.{Row, SparkSession}

object SparkApp {

  def main(args: Array[String]) {

    val sc: SparkSession = SparkSession
      .builder
      .appName("TPCDS_Spark")
      .getOrCreate()

    for(arg <- args){
      // scalastyle:off println
      println(arg)
      // scalastyle:on println
    }

    val sparkSqlProps = args(0).split(",")
    val dataLocation = args(1)
    val queries = args(2).split(",").toSeq
    val queryPath = args(3)
    var buckets_ColumnTable = args(4).toInt
    var isResultCollection = args(5).toBoolean
    var warmUp = args(6).toInt
    var runsForAverage = args(7).toInt
    var cacheTables = args(8).toBoolean

    for (prop <- sparkSqlProps) {
      // scalastyle:off println
      println(prop)
      // scalastyle:on println
      sc.sql(s"set $prop")
    }

    val snc = sc.sqlContext

    for (prop <- sparkSqlProps) {
      snc.sql(s"set $prop")
    }

    // scalastyle:off println
    println(s"****************queries : $queries")
    // scalastyle:on println


    val tables = Seq("call_center", "catalog_page", "date_dim", "household_demographics",
      "income_band", "promotion", "reason", "ship_mode", "store", "time_dim",
      "warehouse", "web_page" , "web_site", "item", "customer_demographics")

    tables.map { tableName =>

      sc.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      if (cacheTables) {
        snc.cacheTable(tableName)
      }
      val count = sc.table(tableName).count()
      tableName -> count

      // scalastyle:off println
      println("-----------------------------------------------")
      println(s"Table Created...$tableName with $count rows")
      println("-----------------------------------------------")
      // scalastyle:on println
    }

    /*
    catalog_returns cr1.cr_order_number **cr_order_number**
    catalog_sales  (cs1.cs_order_number, cs_item_sk, cs_bill_customer_sk) **cs_order_number**
    customer (c_customer_sk , c_current_addr_sk) **c_customer_sk**
    customer_addr (ca_address_sk)  **ca_address_sk**
    inventory  (i_item_sk) **i_item_sk**
    store_returns (sr_item_sk, sr_customer_sk) **sr_item_k**
    store_sales  (customer_sk, address_sk, ss_item_sk) **ss_item_k**
    web_returns 	wr.wr_order_number **wr_order_number**
    web_sales (customer_sk, ws_bill_customer_sk , ws.ws_order_number) **.ws_order_number***
    */

    var partitionBy : String = "cr_order_number"
    var tableName : String = "catalog_returns"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "cs_order_number"
    tableName = "catalog_sales"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "c_customer_sk"
    tableName = "customer"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "ca_address_sk"
    tableName = "customer_address"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "inv_item_sk"
    tableName = "inventory"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "sr_item_sk"
    tableName = "store_returns"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "ss_item_sk"
    tableName = "store_sales"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "wr_order_number"
    tableName = "web_returns"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)

    partitionBy = "ws_order_number"
    tableName = "web_sales"
    createPartitionedTable(sc, dataLocation, partitionBy, tableName, buckets_ColumnTable, cacheTables)


    var avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"Spark_Average.out"))
    var avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    queries.foreach { name =>

      try {

        val path: String = s"$queryPath/$name.sql"
        val queryString = fileToString(new File(path))

        var totalTime: Long = 0

        // scalastyle:off println
        println(s"Running Query $name now.")

        for (i <- 1 to (warmUp + runsForAverage)) {
          val startTime = System.currentTimeMillis()
          var cnts: Array[Row] = null
          if (i == 1) {
            QueryExecutor.planPrintStream = avgPrintStream
            cnts = QueryExecutor.queryExecution(name, queryString, sc.sqlContext, false)._1
          } else {
            cnts = QueryExecutor.queryExecution(name, queryString, sc.sqlContext)._1
          }
          for (s <- cnts) {
            // just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime

          println(s"iterationTime = $iterationTime")

          if (i > warmUp) {
            totalTime += iterationTime
          }
          cnts = null
        }

        // println(s"${totalTime / runsForAverage}")
        println("-----------------------------------------------")
        avgPrintStream.println(s"$name, executionTime = ${totalTime / runsForAverage}")
        println("-----------------------------------------------")

      }
      catch {
        case e: Exception => println(s"Failed query $name  " + e.printStackTrace())
      }
    }

  }


  def createPartitionedTable(sc: SparkSession, dataLocation: String,
                             partitionBy: String, tableName: String, buckets: Int,
                             cacheTables: Boolean = true): Unit = {
    val df = sc.sqlContext.read.parquet(s"$dataLocation/$tableName")
    df.createOrReplaceTempView(tableName)
    df.repartition(buckets, df(partitionBy)).createOrReplaceTempView(tableName)
    df.createOrReplaceTempView(tableName)
    if (cacheTables) {
      sc.sqlContext.cacheTable(tableName)
    }
    // tableName -> sc.table(tableName).count()
    val count = sc.table(tableName).count()

    // scalastyle:off println
    println("-----------------------------------------------")
    println(s"Table $tableName created with $count rows")
    println("-----------------------------------------------")
    // scalastyle:on println
  }

}
