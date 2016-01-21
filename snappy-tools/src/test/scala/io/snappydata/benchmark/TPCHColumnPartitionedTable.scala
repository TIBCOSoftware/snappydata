/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.benchmark

import java.sql.{Date, Statement}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.sql.snappy._

object TPCHColumnPartitionedTable  {

  def createOrderTable_Memsql(stmt: Statement): Unit = {
    stmt.execute("CREATE TABLE ORDERS  ( " +
        "O_ORDERKEY       INTEGER NOT NULL," +
        "O_CUSTKEY        INTEGER NOT NULL," +
        "O_ORDERSTATUS    CHAR(1) NOT NULL," +
        "O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
        "O_ORDERDATE      DATE NOT NULL," +
        "O_ORDERPRIORITY  CHAR(15) NOT NULL," +
        "O_CLERK          CHAR(15) NOT NULL," +
        "O_SHIPPRIORITY   INTEGER NOT NULL," +
        "O_COMMENT        VARCHAR(79) NOT NULL," +
        //"KEY (O_ORDERDATE) USING CLUSTERED COLUMNSTORE," +
        "SHARD KEY(O_ORDERKEY))"
    )
    println("Created Table ORDERS")
  }

  def createAndPopulateOrderTable(props: Map[String, String], sc: SparkContext, path: String, isSnappy: Boolean): Unit = {
    val snappyContext = SnappyContext.getOrCreate(sc)
    val orderData = sc.textFile(s"$path/orders.tbl")
    val orderReadings = orderData.map(s => s.split('|')).map(s => parseOrderRow(s))
    val orderDF = snappyContext.createDataFrame(orderReadings)
    if (isSnappy) {
      val p1 = props + (("PARTITION_BY"-> "o_orderkey"))
      snappyContext.dropTable("ORDERS", ifExists = true)
      snappyContext.createTable("ORDERS", "column", orderDF.schema, p1)
      orderDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("ORDERS")
      //orderDF.registerAndInsertIntoExternalStore("ORDERS", props)
      println("Created Table ORDERS")
    } else {
      orderDF.registerTempTable("ORDERS")
      snappyContext.cacheTable("ORDERS")
      val cnts = snappyContext.sql("select count(*) from ORDERS").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createLineItemTable_Memsql(stmt: Statement): Unit = {
    stmt.execute("CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,"+
        "L_PARTKEY     INTEGER NOT NULL,"+
        "L_SUPPKEY     INTEGER NOT NULL,"+
        "L_LINENUMBER  INTEGER NOT NULL,"+
        "L_QUANTITY    DECIMAL(15,2) NOT NULL,"+
        "L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,"+
        "L_DISCOUNT    DECIMAL(15,2) NOT NULL,"+
        "L_TAX         DECIMAL(15,2) NOT NULL,"+
        "L_RETURNFLAG  CHAR(1) NOT NULL,"+
        "L_LINESTATUS  CHAR(1) NOT NULL,"+
        "L_SHIPDATE    DATE NOT NULL,"+
        "L_COMMITDATE  DATE NOT NULL,"+
        "L_RECEIPTDATE DATE NOT NULL,"+
        "L_SHIPINSTRUCT CHAR(25) NOT NULL,"+
        "L_SHIPMODE     CHAR(10) NOT NULL,"+
        "L_COMMENT      VARCHAR(44) NOT NULL,"+
        //"KEY (L_SHIPDATE) USING CLUSTERED COLUMNSTORE,"+
        "SHARD KEY (L_ORDERKEY)) "
    )

    println("Created Table LINEITEM")
  }

  def createAndPopulateLineItemTable(props: Map[String, String], sc: SparkContext, path:String, isSnappy:Boolean): Unit = {
    val snappyContext = SnappyContext.getOrCreate(sc)
    val lineItemData = sc.textFile(s"$path/lineitem.tbl")
    val lineItemReadings = lineItemData.map(s => s.split('|')).map(s => parseLineItemRow(s))
    val lineOrderDF = snappyContext.createDataFrame(lineItemReadings)
    if (isSnappy) {
      val p1 = props + (("PARTITION_BY"-> "l_orderkey"),("COLOCATE_WITH"->"ORDERS"))

      snappyContext.dropTable("LINEITEM", ifExists = true)
      snappyContext.createTable("LINEITEM", "column", lineOrderDF.schema, p1)
      lineOrderDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("LINEITEM")
      //    lineOrderDF.registerAndInsertIntoExternalStore("LINEITEM", props)
      println("Created Table LINEITEM")
    } else {
      lineOrderDF.registerTempTable("LINEITEM")
      snappyContext.cacheTable("LINEITEM")
      var cnts = snappyContext.sql("select count(*) from LINEITEM").collect()
      for (s <- cnts) {
        var output = s.toString()
        println(output)
      }
    }
  }

  def createAndPopulateOrderSampledTable(props: Map[String, String],
      sc: SparkContext, path: String): Unit = {
    val snappyContext = SnappyContext.getOrCreate(sc)
    val orderDF = snappyContext.table("ORDERS")
    val orderSampled = orderDF.stratifiedSample(Map(
      "qcs" -> "O_ORDERDATE", // O_SHIPPRIORITY
      "fraction" -> 0.03,
      "strataReservoirSize" -> 50))
    orderSampled.registerTempTable("ORDERS_SAMPLED")
    snappyContext.cacheTable("orders_sampled")
    println("Created Sampled Table ORDERS_SAMPLED " + snappyContext.sql(
      "select count(*) as sample_count from orders_sampled").collectAsList())
  }

  def createAndPopulateLineItemSampledTable(props: Map[String, String],
      sc: SparkContext, path: String): Unit = {
    val snappyContext = SnappyContext.getOrCreate(sc)
    val lineOrderDF = snappyContext.table("LINEITEM")
    val lineOrderSampled = lineOrderDF.stratifiedSample(Map(
      "qcs" -> "L_SHIPDATE", // L_RETURNFLAG
      "fraction" -> 0.03,
      "strataReservoirSize" -> 50))
    println(" Logic relation while creation " + lineOrderSampled.logicalPlan.output)
    lineOrderSampled.registerTempTable("LINEITEM_SAMPLED")
    snappyContext.cacheTable("lineitem_sampled")
    println("Created Sampled Table LINEITEM_SAMPLED " + snappyContext.sql(
      "select count(*) as sample_count from lineitem_sampled").collectAsList())
  }

  case class StreamMessageOrderObject(
      o_orderkey:Int,
      o_custkey:Int,
      o_orderstatus:String,
      o_totalprice:Double,
      o_orderdate:Date,
      o_orderpriority:String,
      o_clerk:String,
      o_shippriority:Int,
      o_comment:String
      )

  def  parseOrderRow(s: Array[String]): StreamMessageOrderObject = {
    StreamMessageOrderObject(
      s(0).toInt,
      s(1).toInt,
      s(2),
      s(3).toDouble,
      formatDate(s(4)),
      s(5),
      s(6),
      s(7).toInt,
      s(8)
    )
  }

//  case class StreamMessageLineItemObject(
//      L_ORDERKEY:Int,
//      L_PARTKEY:Int,
//      L_SUPPKEY:Int,
//      L_LINENUMBER:Int,
//      L_QUANTITY:Double,
//      L_EXTENDEDPRICE:Double,
//      L_DISCOUNT:Double,
//      L_TAX:Double,
//      L_RETURNFLAG:String,
//      L_LINESTATUS:String,
//      L_SHIPDATE:Date,
//      L_COMMITDATE:Date,
//      L_RECEIPTDATE:Date,
//      L_SHIPINSTRUCT:String,
//      L_SHIPMODE:String,
//      L_COMMENT:String
//      )

  case class StreamMessageLineItemObject(
      l_orderkey:Int,
      l_partkey:Int,
      l_suppkey:Int,
      l_linenumber:Int,
      l_quantity:Double,
      l_extendedprice:Double,
      l_discount:Double,
      l_tax:Double,
      l_returnflag:String,
      l_linestatus:String,
      l_shipdate:Date,
      l_commitdate:Date,
      l_receiptdate:Date,
      l_shipinstruct:String,
      l_shipmode:String,
      l_comment:String
      )

  def parseLineItemRow(s: Array[String]): StreamMessageLineItemObject = {
    StreamMessageLineItemObject(
      s(0).toInt,
      s(1).toInt,
      s(2).toInt,
      s(3).toInt,
      s(4).toDouble,
      s(5).toDouble,
      s(6).toDouble,
      s(7).toDouble,
      s(8),
      s(9),
      formatDate(s(10)),
      formatDate(s(11)),
      formatDate(s(12)),
      s(13),
      s(14),
      s(15)
    )
  }

  def formatDate(dateString:String): Date = {
    val splittedDate = dateString.split("-")
    val year = splittedDate(0)
    val month = splittedDate(1)
    val day= splittedDate(2)
    new Date((year.toInt - 1900), (month.toInt -1), day.toInt)
  }

}
