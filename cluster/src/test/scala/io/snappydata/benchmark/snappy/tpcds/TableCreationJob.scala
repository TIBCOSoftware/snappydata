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
package io.snappydata.benchmark.snappy.tpcds

import java.io.{File, FileOutputStream, PrintStream}

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object TableCreationJob extends SnappySQLJob{

  var sqlSparkProperties: Array[String] = _
  var dataLocation: String = _
  var buckets_ColumnTable: String = _

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    snc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000");
    val isSnappy = true

    val loadPerfFileStream: FileOutputStream = new FileOutputStream(new File("Snappy_LoadPerf.csv"))
    val loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)
    loadPerfPrintStream.println("Table,RowCount,CreationTimeInMilliSeconds")

    for (prop <- sqlSparkProperties) {
      snc.sql(s"set $prop")
    }

    val tables = Seq("call_center", "catalog_page", "date_dim", "household_demographics",
      "income_band", "promotion", "reason", "ship_mode", "store", "time_dim",
      "warehouse", "web_page" , "web_site")
    // item (~100k rows, 8 buckets) & customer_demographics (2 Million rows) are now column tables

    tables.map { tableName =>
      println(s"Table Creation Started...$tableName")
      val df = snSession.read.parquet(s"$dataLocation/$tableName")
      snSession.dropTable(tableName, ifExists = true)
      val startTime = System.currentTimeMillis()
      snSession.createTable(tableName, "row",
        new StructType(df.schema.map(_.copy(nullable = true)).toArray),
        Map[String, String] ())
      df.write.insertInto(tableName)
      val endTime = System.currentTimeMillis()
      //val cnt = df.collect().length; // avoid collect.length since it's slower than df.count
      val cnt = df.count();
      // scalastyle:off println
      // Get the number of rows in the table created by querying
      val rowCount = snSession.sql(s"SELECT COUNT(*) FROM $tableName").first().get(0)
      println("-----------------------------------------------")
      println(s"Table $tableName with $rowCount rows created from " +
          s"file $dataLocation/$tableName with $cnt rows")
      println("-----------------------------------------------")
      loadPerfPrintStream.println(s"$tableName,$rowCount,${endTime - startTime}")
    }

    var partitionBy : String = ""

    // partitionBy = "cr_order_number"
    partitionBy = "cr_returned_date_sk"
    var props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable))
    var tableName = "catalog_returns"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    // partitionBy = "cs_order_number"
    partitionBy = "cs_sold_date_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "CATALOG_RETURNS"))
    tableName = "catalog_sales"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    // partitionBy = "wr_order_number"
    partitionBy = "wr_returned_date_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "CATALOG_SALES"))
    tableName = "web_returns"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    // partitionBy = "ws_order_number"
    partitionBy = "ws_sold_date_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "WEB_RETURNS"))
    tableName = "web_sales"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "inv_item_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "inventory"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "sr_item_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "INVENTORY"))
    tableName = "store_returns"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "ss_item_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable),
      ("COLOCATE_WITH" -> "STORE_RETURNS"))
    tableName = "store_sales"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "c_customer_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "customer"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "ca_address_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "customer_address"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "cd_demo_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> buckets_ColumnTable))
    tableName = "customer_demographics"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    partitionBy = "i_item_sk"
    props = Map(("PARTITION_BY" -> partitionBy), ("BUCKETS" -> "8"))
    tableName = "item"
    createColumnPartitionedTable(snSession, props, tableName, loadPerfPrintStream)

    // cleanup
    loadPerfPrintStream.flush()
    loadPerfFileStream.flush()
    loadPerfPrintStream.close()
    loadPerfFileStream.close()

  }

  def createColumnPartitionedTable(snappy: SnappySession, props: Map[String, String],
                                   tableName: String, loadPerfPrintStream: PrintStream): Unit = {

    val df = snappy.read.parquet(s"$dataLocation/$tableName")
    snappy.dropTable(tableName, ifExists = true)
    val startTime = System.currentTimeMillis()
    snappy.createTable(tableName, "column",
      new StructType(df.schema.map(_.copy(nullable = false)).toArray), props)
    df.write.insertInto(tableName)
    val endTime = System.currentTimeMillis()
    // val cnt = df.collect().length
    // collect().length takes a very long time to run and may cause memory pressure
    val cnt = df.count()
    // scalastyle:off println
    val rowCount = snappy.sql(s"SELECT COUNT(*) FROM $tableName").first().get(0)
    println("-----------------------------------------------")
    println(s"Table $tableName with $rowCount rows created from " +
        s"file $dataLocation/$tableName with $cnt rows")
    println("-----------------------------------------------")
    loadPerfPrintStream.println(s"$tableName,$rowCount,${endTime - startTime}")
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
      "/QASNAPPY/TPCH/DATA/1"
    }

    buckets_ColumnTable = if (config.hasPath("Buckets_ColumnTable")) {
      config.getString("Buckets_ColumnTable")
    } else {
      "8"
    }

    SnappyJobValid()
  }
}
