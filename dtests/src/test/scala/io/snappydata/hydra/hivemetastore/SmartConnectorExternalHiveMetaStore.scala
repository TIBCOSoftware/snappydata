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
package io.snappydata.hydra.hivemetastore

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappyContext, SparkSession}

object SmartConnectorExternalHiveMetaStore {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector External Hive MetaStore Job started...")
    val dataLocation = args(0)
    val externalThriftServerHostAndPort = args(1)
    val outputFile = "ValidateJoinQuery" + "_" + "column" +
      System.currentTimeMillis() + "_sparkApp"
    val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ExternalHiveMetaStore")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val beelineClientConnection: Connection = getBeelineClientConnection(externalThriftServerHostAndPort)
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
    dropHiveTables(snc, HiveMetaStoreUtils.dropTable)
    dropSnappyTables(snc, HiveMetaStoreUtils.dropTable)
    dropHiveTables(snc, HiveMetaStoreUtils.dropTable, "HIVE_DB")
    dropSnappyTables(snc, HiveMetaStoreUtils.dropTable, "TIBCO_DB")
    //  Test Case-1
    snc.sql("drop database if exists HIVE_DB")
    snc.sql(HiveMetaStoreUtils.setSnappyInBuiltCatalog)
    snc.sql("drop schema if exists TIBCO_DB")
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
//    alterHiveTable_ChangeTableName(snc, pw)
    pw.flush()
    //  Test Case-2
    createAndDropHiveSchema(snc, beelineClientConnection, dataLocation, pw)
    pw.flush()
    //  Test Case-3
    beelineClientConnection.createStatement().execute(HiveMetaStoreUtils.createDB + "HIVE_DB")
    snc.sql(HiveMetaStoreUtils.setSnappyInBuiltCatalog)
    snc.sql(HiveMetaStoreUtils.createDB + "TIBCO_DB")
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
    createHiveTblsAndLoadData(beelineClientConnection, dataLocation, "HIVE_DB")
    createSnappyTblsAndLoadData(snc, dataLocation, "TIBCO_DB")
    createHiveTblsAndLoadData(beelineClientConnection, dataLocation)
    createSnappyTblsAndLoadData(snc, dataLocation)
    executeQueriesOnHiveTables(snc, beelineClientConnection, dataLocation, pw)
    executeJoinQueriesOnHiveAndSnappy(snc, beelineClientConnection, dataLocation, pw)
    dropHiveTables(snc, HiveMetaStoreUtils.dropTable, "HIVE_DB")
    dropSnappyTables(snc, HiveMetaStoreUtils.dropTable, "TIBCO_DB")
    snc.sql("drop database if exists HIVE_DB")
    snc.sql(HiveMetaStoreUtils.setSnappyInBuiltCatalog)
    snc.sql("drop schema if exists TIBCO_DB")
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
    dropHiveTables(snc, HiveMetaStoreUtils.dropTable)
    dropSnappyTables(snc, HiveMetaStoreUtils.dropTable)
    beelineClientConnection.close()
    pw.flush()
    pw.close()
    println("Smart Connector External Hive MetaStore job is successful finished.")
  }

  def getBeelineClientConnection(externalThriftServerHostAndPort : String): Connection = {
        val beelineClientConnection: Connection = DriverManager.getConnection("jdbc:hive2://" + externalThriftServerHostAndPort,
       "hive", "Snappy!23")
    println("Connection with Beeline established.")
    beelineClientConnection
  }

  def dropHiveTables(snc: SnappyContext, dropTable: String,
                     schema: String = "default"): Unit = {
    snc.sql(dropTable + schema + ".hive_regions")
    snc.sql(dropTable + schema + ".hive_categories")
    snc.sql(dropTable + schema + ".hive_shippers")
    snc.sql(dropTable + schema + ".hive_employees")
    snc.sql(dropTable + schema + ".hive_customers")
    snc.sql(dropTable + schema + ".hive_orders")
    snc.sql(dropTable + schema + ".hive_order_details")
    snc.sql(dropTable + schema + ".hive_products")
    snc.sql(dropTable + schema + ".hive_suppliers")
    snc.sql(dropTable + schema + ".hive_territories")
    snc.sql(dropTable + schema + ".hive_employee_territories")
  }

  def dropSnappyTables(snc: SnappyContext, dropTable: String, schema: String = "app"): Unit = {
    snc.sql(dropTable + schema + ".staging_regions")
    snc.sql(dropTable + schema + ".snappy_regions")
    snc.sql(dropTable + schema + ".staging_categories")
    snc.sql(dropTable + schema + ".snappy_categories")
    snc.sql(dropTable + schema + ".staging_shippers")
    snc.sql(dropTable + schema + ".snappy_shippers")
    snc.sql(dropTable + schema + ".staging_employees")
    snc.sql(dropTable + schema + ".snappy_employees")
    snc.sql(dropTable + schema + ".staging_customers")
    snc.sql(dropTable + schema + ".snappy_customers")
    snc.sql(dropTable + schema + ".staging_orders")
    snc.sql(dropTable + schema + ".snappy_orders")
    snc.sql(dropTable + schema + ".staging_order_details")
    snc.sql(dropTable + schema + ".snappy_order_details")
    snc.sql(dropTable + schema + ".staging_products")
    snc.sql(dropTable + schema + ".snappy_products")
    snc.sql(dropTable + schema + ".staging_suppliers")
    snc.sql(dropTable + schema + ".snappy_suppliers")
    snc.sql(dropTable + schema + ".staging_territories")
    snc.sql(dropTable + schema + ".snappy_territories")
    snc.sql(dropTable + schema + ".staging_employee_territories")
    snc.sql(dropTable + schema + ".snappy_employee_territories")
  }

  def createHiveTable(tableDef: String, beelineClientConnection: Connection, schema: String): Unit = {
    beelineClientConnection.createStatement().execute("create table " + schema + "." + tableDef
      + " row format delimited fields terminated by ',' ")
  }

  def loadDataToHiveTbls(path: String, tblName: String,
                         beelineConn: Connection, schema: String): Unit = {
    beelineConn.createStatement().execute("load data local inpath '" + path
      + "' overwrite into table " + schema + "." + tblName)
  }

  def createHiveTblsAndLoadData(beelineClientConnection: Connection, dataLocation: String,
                                schema: String = "default"): Unit = {
    createHiveTable("hive_regions(RegionID int,RegionDescription string)",
      beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "regions.csv", "hive_regions", beelineClientConnection, schema)
    createHiveTable("hive_categories" +
      "(CategoryID int,CategoryName string,Description string,Picture string)",
      beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "categories.csv", "hive_categories",
      beelineClientConnection, schema)
    createHiveTable("hive_shippers(ShipperID int ,CompanyName string ,Phone string)",
      beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "shippers.csv", "hive_shippers", beelineClientConnection, schema)
    createHiveTable("hive_employees(EmployeeID int,LastName string,FirstName string,Title string," +
      "TitleOfCourtesy string,BirthDate timestamp,HireDate timestamp,Address string," +
      "City string,Region string,PostalCode string,Country string," +
      "HomePhone string,Extension string,Photo string," +
      "Notes string,ReportsTo int,PhotoPath string)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "employees.csv", "hive_employees", beelineClientConnection, schema)
    createHiveTable("hive_customers(CustomerID string,CompanyName string,ContactName string," +
      "ContactTitle string,Address string,City string,Region string," +
      "PostalCode string,Country string,Phone string,Fax string)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "customers.csv", "hive_customers", beelineClientConnection, schema)
    createHiveTable("hive_orders(OrderID int,CustomerID string,EmployeeID int," +
      "OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp," +
      "ShipVia int,Freight double,ShipName string,ShipAddress string,ShipCity string," +
      "ShipRegion string,ShipPostalCode string,ShipCountry string)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "orders.csv", "hive_orders", beelineClientConnection, schema)
    createHiveTable("hive_order_details(OrderID int,ProductID int,UnitPrice " +
      "double,Quantity smallint,Discount double)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "order_details.csv", "hive_order_details",
      beelineClientConnection, schema)
    createHiveTable("hive_products(ProductID int,ProductName string,SupplierID int," +
      "CategoryID int,QuantityPerUnit string,UnitPrice double,UnitsInStock smallint," +
      "UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint)",
      beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "products.csv", "hive_products", beelineClientConnection, schema)
    createHiveTable("hive_suppliers(SupplierID int,CompanyName string,ContactName string," +
      "ContactTitle string,Address string,City string,Region string," +
      "PostalCode string,Country string,Phone string," +
      "Fax string,HomePage string)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "suppliers.csv", "hive_suppliers", beelineClientConnection, schema)
    createHiveTable("hive_territories(TerritoryID string,TerritoryDescription string," +
      "RegionID string)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "territories.csv", "hive_territories",
      beelineClientConnection, schema)
    createHiveTable("hive_employee_territories(EmployeeID int," +
      "TerritoryID string)", beelineClientConnection, schema)
    loadDataToHiveTbls(dataLocation + "employee_territories.csv",
      "hive_employee_territories", beelineClientConnection, schema)
  }

  def createSnappyTblsAndLoadData(snc: SnappyContext, dataLocation: String,
                                  schema: String = "app"): Unit = {
    snc.sql("create external table if not exists " + schema + "." + "staging_regions using csv" +
      " options(path '" + "file:///" + dataLocation + "regions.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_categories using csv" +
      " options(path '" + "file:///" + dataLocation + "categories.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_shippers using csv" +
      " options(path '" + "file:///" + dataLocation + "shippers.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_employees using csv" +
      " options(path '" + "file:///" + dataLocation + "employees.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_customers using csv" +
      " options(path '" + "file:///" + dataLocation + "customers.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_orders using csv" +
      " options(path '" + "file:///" + dataLocation + "orders.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." +
      "staging_order_details using csv options(path '" +
      "file:///" + dataLocation + "order_details.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_products using csv" +
      " options(path '" + "file:///" + dataLocation + "products.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." + "staging_suppliers using csv" +
      " options(path '" + "file:///" + dataLocation + "suppliers.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema +
      "." + "staging_territories using csv options(path '" +
      "file:///" + dataLocation + "territories.csv" + "',header 'true')")
    snc.sql("create external table if not exists " + schema + "." +
      "staging_employee_territories using csv options(path '" +
      "file:///" + dataLocation + "employee_territories.csv" + "',header 'true')")

    snc.sql("create table if not exists " + schema + "." + "snappy_regions using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_regions")
    snc.sql("create table if not exists " + schema + "." + "snappy_categories using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_categories")
    snc.sql("create table if not exists " + schema + "." + "snappy_shippers using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_shippers")
    snc.sql("create table if not exists " + schema + "." + "snappy_employees using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_employees")
    snc.sql("create table if not exists " + schema + "." + "snappy_customers using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_customers")
    snc.sql("create table if not exists " + schema + "." + "snappy_orders using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_orders")
    snc.sql("create table if not exists " + schema + "." + "snappy_order_details using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_order_details")
    snc.sql("create table if not exists " + schema + "." + "snappy_products using column" +
      " options(BUCKETS '8') as select * from " + schema + "." +  "staging_products")
    snc.sql("create table if not exists " + schema + "." + "snappy_suppliers using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_suppliers")
    snc.sql("create table if not exists " + schema + "." + "snappy_territories using column" +
      " options(BUCKETS '8') as select * from " + schema + "." + "staging_territories")
    snc.sql("create table if not exists " + schema + "." +
      "snappy_employee_territories using column options(BUCKETS '8') " +
      "as select * from " + schema + "." + "staging_employee_territories")
  }

  def executeQueries(snc: SnappyContext, query1: String, query2: String, pw: PrintWriter,
                     index: Int, id : Int): Unit = {
    var isDiff1: Boolean = false
    var isDiff2: Boolean = false
    pw.println("Query" + index + " : " + query1)
    val df1 = snc.sql(query1)
    if(id == 0) {
      pw.println("Hive Query executed from Snappy count: " + df1.count())
    }
    if(id ==1) {
      pw.println("Hive Join Snappy  Count: " + df1.count())
    }
    val df2 = snc.sql(query2)
    if(id == 0) {
      pw.println("Snappy Query Count (Validation) : " + df2.count())
    }
    if(id == 1) {
      pw.println("Snappy Join Snappy Count (Validation) : " + df2.count())
    }
    val diff1 = df1.except(df2)
    if (diff1.count() > 0) {
      diff1.write.csv("file:///" +
        System.getProperty("user.dir") + "/diff1_" + id + "_" + index + ".csv")
    } else {
      isDiff1 = true
    }
    val diff2 = df2.except(df1)
    if (diff2.count() > 0) {
      diff2.write.csv("file:///" +
        System.getProperty("user.dir") + "/diff2_" + id + "_" +  index + ".cvs")
    } else {
      isDiff2 = true
    }
    if (isDiff1 && isDiff2) {
      if (id == 0) {
        pw.println("For Query " + index + " Hive query Passed.")
      }
      if(id == 1) {
        pw.println("For Query " + index + " Join between Hive and Snappy is Passed.")
      }
    }
    else {
      if (id == 0) {
        pw.println("For Query " + index + " Hive Query execution is not successful")
      }
      if(id == 1) {
        pw.println("For Query " + index + " Join between Hive and Snappy is not successful")
      }
    }
    isDiff1 = false
    isDiff2 = false
    pw.println("* * * * * * * * * * * * * * * * * * * * * * * * *")
  }

  def alterHiveTable_ChangeTableName(snc: SnappyContext, pw: PrintWriter): Unit = {
    snc.sql(HiveMetaStoreUtils.dropTable + "default.Table1")
    snc.sql(HiveMetaStoreUtils.dropTable + "default.Table2")
    snc.sql("create table if not exists default.Table1(id int, name String) using hive")
    snc.sql("insert into default.Table1 select id, concat('TIBCO_',id) from range(100000)")
    snc.sql("alter table default.Table1 rename to default.Table2")
    val countDF = snc.sql("select count(*) as Total from default.Table2")
    println("countDF : " + countDF.head())
    val count = countDF.head().toString()
      .replace("[", "")
      .replace("]", "").toLong
    if (count.==(100000)) {
      pw.println("Create the table in beeline from snappy," +
        " \n insert data into it from snappy," +
        " \n rename the table name from snappy," +
        " \n count the no. of records from snappy and " +
        "dropping the beeline table from snappy" +
        "\n is successful")
      pw.println("Alter table test passed.")
    }
    pw.println("* * * * * * * * * * * * * * * * * * * * * * * * *")
    snc.dropTable("default.Table1", true)
    snc.dropTable("default.Table2", true)
  }

  def createAndDropHiveSchema(snc: SnappyContext, beelineClientConnection: Connection,
                              dataLocation: String, pw: PrintWriter): Unit = {
    var isDiff1 = false
    var isDiff2 = false
    snc.sql(HiveMetaStoreUtils.dropTable + "hiveDB.hive_regions")
    snc.sql(HiveMetaStoreUtils.dropTable + "snappyDB.staging_regions")
    snc.sql(HiveMetaStoreUtils.dropTable + "snappyDB.snappy_regions")
    snc.sql("drop database if exists hiveDB")
    snc.sql("drop schema if exists snappyDB")
    snc.sql("create database hiveDB")
    snc.sql(HiveMetaStoreUtils.setSnappyInBuiltCatalog)
    snc.sql("create schema snappyDB")
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
    createHiveTable("hive_regions(RegionID int,RegionDescription string)", beelineClientConnection,
      "hiveDB")
    loadDataToHiveTbls(dataLocation + "regions.csv", "hive_regions", beelineClientConnection, "hiveDB")
    snc.sql("create external table if not exists snappyDB.staging_regions using csv" +
      " options(path '" + "file:///" + dataLocation + "regions.csv" + "',header 'true')")
    snc.sql("create table if not exists snappyDB.snappy_regions using column" +
      " options(BUCKETS '10') as select * from snappyDB.staging_regions")
    val df1 = snc.sql("select * from hiveDB.hive_regions " +
      "where RegionDescription <> 'RegionDescription'")
    pw.println("Hive Table Count : " + df1.count())
    val df2 = snc.sql("select * from snappyDB.snappy_regions")
    pw.println("Snappy Table Count : " + df2.count())
    val diff1 = df1.except(df2)
    if (diff1.count() > 0) {
      diff1.write.csv("file:///" +
        System.getProperty("user.dir") + "/diff1_HiveTable" + ".cvs")
    } else {
      isDiff1 = true
    }
    val diff2 = df2.except(df1)
    if (diff2.count() > 0) {
      diff1.write.csv("file:///" +
        System.getProperty("user.dir") + "/diff1_SnappyTable" + ".cvs")
    } else {
      isDiff2 = true
    }
    if (isDiff1 && isDiff2) {
      pw.println("Hive Table is same as Snappy Table")
    }
    else {
      pw.println("Hive Table is not same as Snappy Table")
    }
    isDiff1 = false
    isDiff2 = false
    pw.println("* * * * * * * * * * * * * * * * * * * * * * * * *")
    snc.sql(HiveMetaStoreUtils.dropTable + "hiveDB.hive_regions")
    snc.sql(HiveMetaStoreUtils.dropTable + "snappyDB.staging_regions")
    snc.sql(HiveMetaStoreUtils.dropTable + "snappyDB.snappy_regions")
    snc.sql("drop database if exists hiveDB")
    snc.sql(HiveMetaStoreUtils.setSnappyInBuiltCatalog)
    snc.sql("drop schema if exists snappyDB")
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
  }

  def executeQueriesOnHiveTables(snc : SnappyContext, beelineClientConnection : Connection,
                                 dataLocation : String, pw : PrintWriter): Unit = {
    for(index <- 0 to HiveMetaStoreUtils.beeLineQueries.length-1) {
      executeQueries(snc, HiveMetaStoreUtils.beeLineQueries(index),
        HiveMetaStoreUtils.snappyQueries(index), pw, index, 0)
    }
  }

  def executeJoinQueriesOnHiveAndSnappy(snc : SnappyContext, beelineClientConnection : Connection,
                                        dataLocation : String, pw : PrintWriter) : Unit = {
    for (index <- 0 to 4) {
       executeQueries(snc, HiveMetaStoreUtils.joinHiveSnappy(index),
        HiveMetaStoreUtils.validateJoin(index), pw, index, 1)
      pw.flush()
    }
  }
}
