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
package io.snappydata.hydra.security

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.northwind.{NWQueries, NWTestUtil}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SnappyContext

object CreateAndLoadTablesSparkApp {

  def main(args: Array[String]) {
    // scalastyle:off println
    val connectionURL = args(args.length - 1)
    println("The connection url is " + connectionURL)
    val conf = new SparkConf().
        setAppName("CreateAndLoadNWTablesSpark Application").
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadNWTablesSparkApp.out"),
      true));
   val dataFilesLocation = args(0)
    // snc.sql("set spark.sql.shuffle.partitions=6")
    snc.setConf("dataFilesLocation", dataFilesLocation)
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
   // val tableType = args(1)

    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    NWTestUtil.dropTables(snc)
  // pw.println(s"Create and load ${tableType} tables Test started at : " +
    // System.currentTimeMillis)
    createColRowTables(snc)
  /*  tableType match {
      case "ReplicatedRow" => NWTestUtil.createAndLoadReplicatedTables(snc)
      case "PartitionedRow" => NWTestUtil.createAndLoadPartitionedTables(snc)
      case "Column" => NWTestUtil.createAndLoadColumnTables(snc)
      case "Colocated" => NWTestUtil.createAndLoadColocatedTables(snc)
      case _ => // the default, catch-all
    } */
    // pw.println(s"Create and load ${tableType} tables Test completed successfully at : " + System
      //  .currentTimeMillis)
    println("Getting users arguments")
    val queryFile = args(1)
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val isGrant = args(2).toBoolean
    val userSchema1 = args(3)
    val userSchema2 = args(4)
    println("The arguments are queryFile " + queryFile + " isGrant = " +
        isGrant + "userSchema = " + userSchema1 + " " + userSchema2)
    pw.println("The arguments are queryFile " + queryFile + " isGrant = " +
        isGrant + "userSchema = " + userSchema1 + " " + userSchema2)

    val expectedExcptCnt = args(5).toInt
    val unExpectedExcptCnt = args(6).toInt
    val userSchema = new Array[String](2)
    userSchema(0) = userSchema1;
    userSchema(1) = userSchema2;

   // val splitStr = userSchema.split(" ")
   // val currentUser = args(7)
    // if(isGrant){
     /* val qry1 = "SELECT count(*) from " + userSchema(0)
       snc.sql(qry1).show
      val qry2 = "select * from user2.suppliers"
       snc.sql(qry2).show */
      SecurityTestUtil.runQueries(snc, queryArray, expectedExcptCnt, unExpectedExcptCnt,
        isGrant, userSchema, pw)
    // }
    pw.close()
  }

  def createColRowTables(snc: SnappyContext) : Unit = {
    println("Inside createColAndRow tables")
    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.employees_table + " using column options(partition_by 'City,Country', " +
        "redundancy '1')")
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table +
        " using row options( partition_by 'PostalCode,Region', buckets '19', colocate_with " +
        "'employees', redundancy '1')")
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using column options (partition_by 'OrderId', buckets " +
        "'13', redundancy '1')")
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
        " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', " +
        "redundancy '1')")
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " USING column options (partition_by 'ProductID,SupplierID', buckets '17', redundancy '1')")
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '123', redundancy '1')")
    NWQueries.suppliers(snc).write.insertInto("suppliers")

  }
}
