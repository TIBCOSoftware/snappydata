/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.benchmark.snappy.tpch


import java.io.{File, FileOutputStream, PrintStream}

import com.typesafe.config.Config
import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.benchmark.TPCDSQuerySnappyBenchmark
import org.apache.spark.sql.types.StructType

/**
  * Created by sbhokare on 1/9/17.
  */
class QueryExecutionTPCDS extends SnappySQLJob with Logging{

  var sqlSparkProperties: Array[String] = _
  var dataLocation: String = _
  var queries: Array[String] = _
  var queryPath: String = _

//  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
//    val snc = snSession.sqlContext
//
//
//    for (prop <- sqlSparkProperties) {
//      snc.sql(s"set $prop")
//    }
//
//    TPCDSQuerySnappyBenchmark.snappy = snSession
//    TPCDSQuerySnappyBenchmark.execute(dataLocation, queries, true, queryPath)
//
//  }

  def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    for (prop <- sqlSparkProperties) {
      snc.sql(s"set $prop")
    }

    // scalastyle:off println
    //println(s"****************queries : $queries")
    // scalastyle:on println

    /*
      call_center
      catalog_page
      date_dim
      household_demographics
      income_band
      promotions
      reasons
      ship_mode
      store
      time_dim
      warehouse
      webpage
      website
   */

    /*catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page"*/

    val tables = Seq("call_center", "catalog_page", "date_dim", "household_demographics",
      "income_band", "promotion", "reason", "ship_mode", "store", "time_dim",
      "warehouse", "web_page" , "web_site", "item", "customer_demographics")

    tables.map { tableName =>
      //println(s"Table Creation Started...$tableName")
      val df = snSession.read.parquet(s"$dataLocation/$tableName")
      snSession.createTable(tableName, "row",
        new StructType(df.schema.map(_.copy(nullable = true)).toArray),
        Map[String, String] ())
      df.write.insertInto(tableName)
      val cnt = df.collect().length;
      // scalastyle:off println
      println("-----------------------------------------------")
      println(s"Table Created...$tableName with rows $cnt")

      println("-----------------------------------------------")
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

    var props = Map(("PARTITION_BY" -> "cr_order_number"), ("BUCKETS" -> "223"))
    var tableName = "catalog_returns"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "cs_order_number"), ("BUCKETS" -> "223"),
    ("COLOCATE_WITH" -> "CATALOG_RETURNS"))
    tableName = "catalog_sales"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "wr_order_number"), ("BUCKETS" -> "223"),
    ("COLOCATE_WITH" -> "CATALOG_SALES"))
    tableName = "web_returns"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "ws_order_number"), ("BUCKETS" -> "223"),
    ("COLOCATE_WITH" -> "WEB_RETURNS"))
    tableName = "web_sales"
    createColumnPartitionedTables(snSession, props, tableName)


    props = Map(("PARTITION_BY" -> "inv_item_sk"), ("BUCKETS" -> "223"))
    tableName = "inventory"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "sr_item_sk"), ("BUCKETS" -> "223"),
      ("COLOCATE_WITH" -> "INVENTORY"))
    tableName = "store_returns"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "ss_item_sk"), ("BUCKETS" -> "223"),
      ("COLOCATE_WITH" -> "STORE_RETURNS"))
    tableName = "store_sales"
    createColumnPartitionedTables(snSession, props, tableName)


    props = Map(("PARTITION_BY" -> "c_customer_sk"), ("BUCKETS" -> "223"))
    tableName = "customer"
    createColumnPartitionedTables(snSession, props, tableName)

    props = Map(("PARTITION_BY" -> "ca_address_sk"), ("BUCKETS" -> "223"))
    tableName = "customer_address"
    createColumnPartitionedTables(snSession, props, tableName)

    val avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"Snappy_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)


    queries.foreach { name =>

      try {

        val path: String = s"$queryPath/$name.sql"
        val queryString = fileToString(new File(path))
        val warmup = 2
        val runsForAverage = 3

        var totalTime: Long = 0

        // scalastyle:off println
        //println("Query : " + queryString)

        for (i <- 1 to (warmup + runsForAverage)) {
          // queryPrintStream.println(queryToBeExecuted)
          val startTime = System.currentTimeMillis()
          var cnts: Array[Row] = null
          if (i == 1) {
            cnts = QueryExecutor.queryExecution(name, queryString, snSession.sqlContext, true)._1
          } else {
            cnts = QueryExecutor.queryExecution(name, queryString, snSession.sqlContext)._1
          }
          for (s <- cnts) {
            // just iterating over result
          }
          val endTime = System.currentTimeMillis()
          val iterationTime = endTime - startTime

          // scalastyle:off println
          println(s"iterationTime = $iterationTime")

          if (i > warmup) {
            totalTime += iterationTime
          }
          cnts = null
        }

        // scalastyle:off println
        //println(s"${totalTime / runsForAverage}")
        println("-----------------------------------------------")
        avgPrintStream.println(s"$name, executionTime = ${totalTime / runsForAverage}")
        println("-----------------------------------------------")

      }
      catch {
        case e: Exception => println(s"Failed $name  " );
          logError("Exception in job", e);
      }
    }
}


def createColumnPartitionedTables(snappy: SnappySession,
                                  props: Map[String,String] , tableName: String): Unit = {

    val df = snappy.read.parquet(s"$dataLocation/$tableName")
    snappy.createTable(tableName, "column",
      new StructType(df.schema.map(_.copy(nullable = false)).toArray), props)
    df.write.insertInto(tableName)
    val cnt = df.collect().length
  // scalastyle:off println
  println("-----------------------------------------------")
  println(s"Table Created...$tableName with rows $cnt")
  println("-----------------------------------------------")
}

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    val sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "
    sqlSparkProperties = sqlSparkProps.split(",")

    dataLocation = if (config.hasPath("dataDir")) {
      config.getString("dataDir")
    } else {
      ""
    }

    val tempqueries = if (config.hasPath("queries")) {
      config.getString("queries")
    } else {
      return SnappyJobInvalid("Specify Query number to be executed")
    }
    // scalastyle:off println
    println(s"tempqueries : $tempqueries")
    queries = tempqueries.split(",")

    queryPath = if (config.hasPath("queryPath")) {
      config.getString("queryPath")
    } else {
      ""
    }
    SnappyJobValid()
  }

}
