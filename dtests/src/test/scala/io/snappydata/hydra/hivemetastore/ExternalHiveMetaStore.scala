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
import com.typesafe.config.Config
import org.apache.spark.sql._

class ExternalHiveMetaStore extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    println("External Hive MetaStore Embedded mode Job started...")
    val diffPath : String = "file:///home/cbhatt/DiffDir/"
    val dataLocation = jobConfig.getString("dataFilesLocation")
    val outputFile = "ValidateJoinQuery" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val snc : SnappyContext = snappySession.sqlContext
//    val sqlContext : SQLContext = spark.sqlContext

    val beelineConnection : Connection = connectToBeeline()
    snc.sql(HiveMetaStoreUtils.setExternalHiveCatalog)
    dropBeelineTabelsFromSnappy(snc, HiveMetaStoreUtils.dropTable)
    dropSnappyTables(snc, HiveMetaStoreUtils.dropTable)
    alterTableCheck(snc, pw)
    createAndDropSchemaCheck(snc, beelineConnection, dataLocation, pw, diffPath)
    createHiveTblsAndLoadData(beelineConnection, dataLocation)
    createSnappyTblsAndLoadData(snc, dataLocation)
    for(index <- 0 to (HiveMetaStoreUtils.joinHiveSnappy.length-1)) {
      executeJoinQueries(snc, spark, HiveMetaStoreUtils.joinHiveSnappy(index),
        HiveMetaStoreUtils.validateJoin(index), pw, index, diffPath)
    }
    dropBeelineTabelsFromSnappy(snc, HiveMetaStoreUtils.dropTable)
    dropSnappyTables(snc, HiveMetaStoreUtils.dropTable)
    pw.flush()
    pw.close()
    println("External Hive MetaStore Embedded mode job is successful")
  }

  def connectToBeeline() : Connection = {
    val beelineConnection : Connection = DriverManager.getConnection("jdbc:hive2://localhost:11000",
      "APP", "mine");
    println("Connection with Beeline established.")
    beelineConnection
  }

  def dropBeelineTabelsFromSnappy(snc : SnappyContext, dropTable : String): Unit = {
    snc.sql(dropTable + "default.hive_regions")
    snc.sql(dropTable + "default.hive_categories")
    snc.sql(dropTable + "default.hive_shippers")
    snc.sql(dropTable + "default.hive_employees")
    snc.sql(dropTable + "default.hive_customers")
    snc.sql(dropTable + "default.hive_orders")
    snc.sql(dropTable + "default.hive_order_details")
    snc.sql(dropTable + "default.hive_products")
    snc.sql(dropTable + "default.hive_suppliers")
    snc.sql(dropTable + "default.hive_territories")
    snc.sql(dropTable + "default.hive_employee_territories")
  }

  def dropSnappyTables(snc : SnappyContext, dropTable : String) : Unit = {
    snc.sql(dropTable + "app.staging_regions");
    snc.sql(dropTable + "app.snappy_regions");
    snc.sql(dropTable + "app.staging_categories");
    snc.sql(dropTable + "app.snappy_categories");
    snc.sql(dropTable + "app.staging_shippers");
    snc.sql(dropTable + "app.snappy_shippers");
    snc.sql(dropTable + "app.staging_employees");
    snc.sql(dropTable + "app.snappy_employees");
    snc.sql(dropTable + "app.staging_customers");
    snc.sql(dropTable + "app.snappy_customers");
    snc.sql(dropTable + "app.staging_orders");
    snc.sql(dropTable + "app.snappy_orders");
    snc.sql(dropTable + "app.staging_order_details");
    snc.sql(dropTable + "app.snappy_order_details");
    snc.sql(dropTable + "app.staging_products");
    snc.sql(dropTable + "app.snappy_products");
    snc.sql(dropTable + "app.staging_suppliers");
    snc.sql(dropTable + "app.snappy_suppliers");
    snc.sql(dropTable + "app.staging_territories");
    snc.sql(dropTable + "app.snappy_territories");
    snc.sql(dropTable + "app.staging_employee_territories");
    snc.sql(dropTable + "app.snappy_employee_territories");
  }

  def createHiveTable(tableDef : String, beelineConnection : Connection) : Unit = {
    beelineConnection.createStatement().execute("create table " + tableDef
      + " row format delimited fields terminated by ',' ")
  }

  def loadDataToHiveTbls(path: String, tblName: String, beelineConn : Connection) : Unit = {
    beelineConn.createStatement().execute("load data local inpath '" + path
      + "' overwrite into table " + tblName)
  }

  def createHiveTblsAndLoadData(beelineConnection : Connection, dataLocation : String) : Unit = {
    createHiveTable("hive_regions(RegionID int,RegionDescription string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "regions.csv", "hive_regions", beelineConnection)
    createHiveTable("hive_categories" +
      "(CategoryID int,CategoryName string,Description string,Picture string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "categories.csv", "hive_categories", beelineConnection)
    createHiveTable("hive_shippers(ShipperID int ,CompanyName string ,Phone string)",
      beelineConnection)
    loadDataToHiveTbls(dataLocation + "shippers.csv", "hive_shippers", beelineConnection)
    createHiveTable("hive_employees(EmployeeID int,LastName string,FirstName string,Title string," +
      "TitleOfCourtesy string,BirthDate timestamp,HireDate timestamp,Address string," +
      "City string,Region string,PostalCode string,Country string," +
      "HomePhone string,Extension string,Photo string," +
      "Notes string,ReportsTo int,PhotoPath string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "employees.csv", "hive_employees", beelineConnection)
    createHiveTable("hive_customers(CustomerID string,CompanyName string,ContactName string," +
      "ContactTitle string,Address string,City string,Region string," +
      "PostalCode string,Country string,Phone string,Fax string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "customers.csv", "hive_customers", beelineConnection)
    createHiveTable("hive_orders(OrderID int,CustomerID string,EmployeeID int," +
      "OrderDate timestamp,RequiredDate timestamp,ShippedDate timestamp," +
      "ShipVia int,Freight double,ShipName string,ShipAddress string,ShipCity string," +
      "ShipRegion string,ShipPostalCode string,ShipCountry string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "orders.csv", "hive_orders", beelineConnection)
    createHiveTable("hive_order_details(OrderID int,ProductID int,UnitPrice " +
      "double,Quantity smallint,Discount double)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "order-details.csv", "hive_order_details", beelineConnection)
    createHiveTable("hive_products(ProductID int,ProductName string,SupplierID int," +
      "CategoryID int,QuantityPerUnit string,UnitPrice double,UnitsInStock smallint," +
      "UnitsOnOrder smallint,ReorderLevel smallint,Discontinued smallint)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "products.csv", "hive_products", beelineConnection)
    createHiveTable("hive_suppliers(SupplierID int,CompanyName string,ContactName string," +
      "ContactTitle string,Address string,City string,Region string," +
      "PostalCode string,Country string,Phone string," +
      "Fax string,HomePage string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "suppliers.csv", "hive_suppliers", beelineConnection)
    createHiveTable("hive_territories(TerritoryID string,TerritoryDescription string," +
      "RegionID string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "territories.csv", "hive_territories", beelineConnection)
    createHiveTable("hive_employee_territories(EmployeeID int," +
      "TerritoryID string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "employee-territories.csv",
      "hive_employee_territories", beelineConnection)
  }

  def createSnappyTblsAndLoadData(snc : SnappyContext, dataLocation : String) : Unit = {

    snc.sql("create external table if not exists app.staging_regions using csv" +
      " options(path '" + "file:///" + dataLocation + "regions.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_categories using csv" +
      " options(path '" + "file:///" + dataLocation + "categories.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_shippers using csv" +
      " options(path '" + "file:///" + dataLocation + "shippers.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_employees using csv" +
      " options(path '" + "file:///" + dataLocation + "employees.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_customers using csv" +
      " options(path '" + "file:///" + dataLocation + "customers.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_orders using csv" +
      " options(path '" + "file:///" + dataLocation + "orders.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_order_details using csv" +
      " options(path '" + "file:///" + dataLocation + "order-details.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_products using csv" +
      " options(path '" + "file:///" + dataLocation + "products.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_suppliers using csv" +
      " options(path '" + "file:///" + dataLocation + "suppliers.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_territories using csv" +
      " options(path '" + "file:///" + dataLocation + "territories.csv" + "',header 'true')")
    snc.sql("create external table if not exists app.staging_employee_territories using csv" +
      " options(path '" + "file:///" + dataLocation +
      "employee-territories.csv" + "',header 'true')")

    snc.sql("create table if not exists app.snappy_regions using column" +
      " options(BUCKETS '10') as select * from app.staging_regions")
    snc.sql("create table if not exists app.snappy_categories using column" +
      " options(BUCKETS '10') as select * from app.staging_categories")
    snc.sql("create table if not exists app.snappy_shippers using column" +
      " options(BUCKETS '10') as select * from app.staging_shippers")
    snc.sql("create table if not exists app.snappy_employees using column" +
      " options(BUCKETS '64') as select * from app.staging_employees")
    snc.sql("create table if not exists app.snappy_customers using column" +
      " options(BUCKETS '64') as select * from app.staging_customers")
    snc.sql("create table if not exists app.snappy_orders using column" +
      " options(BUCKETS '64') as select * from app.staging_orders")
    snc.sql("create table if not exists app.snappy_order_details using column" +
      " options(BUCKETS '64') as select * from app.staging_order_details")
    snc.sql("create table if not exists app.snappy_products using column" +
      " options(BUCKETS '10') as select * from app.staging_products")
    snc.sql("create table if not exists app.snappy_suppliers using column" +
      " options(BUCKETS '10') as select * from app.staging_suppliers")
    snc.sql("create table if not exists app.snappy_territories using column" +
      " options(BUCKETS '10') as select * from app.staging_territories")
    snc.sql("create table if not exists app.snappy_employee_territories using column" +
      " options(BUCKETS '10') as select * from app.staging_employee_territories")
  }

  def executeJoinQueries(snc : SnappyContext, spark : SparkSession,
                         query1 : String, query2 : String, pw : PrintWriter,
                         index : Int, diffPath : String) : Unit = {
    var isDiff1  : Boolean = false
    var isDiff2 : Boolean = false
    pw.println("Query" + index + " : " + query1)
    val df1 = snc.sql(query1)
    pw.println("Hive Join Snappy  Count: " + df1.count())
    val df2 = snc.sql(query2)
    pw.println("Snappy Join Snappy Count (Validation) : " + df2.count())
    val diff1 = df1.except(df2)
    if(diff1.count() > 0) {
      diff1.write.csv(diffPath + "diff1_" + index + ".csv")
    } else {
      isDiff1 = true
    }
    val diff2 = df2.except(df1)
    if(diff2.count() > 0) {
      diff2.write.csv( diffPath + "diff2_" + index + ".cvs")
    } else {
      isDiff2 = true
    }
    if(isDiff1 && isDiff2) {
      pw.println("For Query " + index + " Join between Hive and Snappy is successful")
    }
    else {
      pw.println("For Query " + index + " Join between Hive and Snappy is not successful")
    }
    isDiff1 = false
    isDiff2 = false
    pw.println("* * * * * * * * * * * * * * * * * * * * * * * * *")
  }

  def alterTableCheck(snc : SnappyContext, pw : PrintWriter): Unit = {
    snc.sql(HiveMetaStoreUtils.dropTable + "default.Table1")
    snc.sql(HiveMetaStoreUtils.dropTable  + "default.Table2")
    snc.sql("create table if not exists default.Table1(id int, name String) using hive")
    snc.sql("insert into default.Table1 select id, concat('TIBCO_',id) from range(100000)")
    snc.sql("alter table default.Table1 rename to default.Table2")
    val countDF = snc.sql("select count(*) as Total from default.Table2")
    println("countDF : " + countDF.head())
    val count = countDF.head().toString()
      .replace( "[" , "")
      .replace(  "]" , "").toLong
    if(count.==(100000)) {
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

  def createAndDropSchemaCheck(snc : SnappyContext, beelineConnection : Connection,
                               dataLocation : String, pw : PrintWriter, diffPath : String): Unit = {
    var isDiff1 = false
    var isDiff2 = false
    snc.sql(HiveMetaStoreUtils.dropTable + "hiveDB.hive_regions")
    snc.sql(HiveMetaStoreUtils.dropTable + "snappyDB.staging_regions")
    snc.sql(HiveMetaStoreUtils.dropTable + "snappyDB.snappy_regions")
    snc.sql("drop database if exists hiveDB")
    snc.sql("drop schema if exists snappyDB")
    snc.sql("create database hiveDB")
    snc.sql("create schema snappyDB")
    createHiveTable("hiveDB.hive_regions(RegionID int,RegionDescription string)", beelineConnection)
    loadDataToHiveTbls(dataLocation + "regions.csv", "hiveDB.hive_regions", beelineConnection)
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
    if(diff1.count() > 0) {
      diff1.write.csv(diffPath + "diff1_HiveTable" + ".cvs")
    } else {
      isDiff1 = true
    }
    val diff2 = df2.except(df1)
    if(diff2.count() > 0) {
      diff1.write.csv(diffPath + "diff1_SnappyTable" + ".cvs")
    } else {
      isDiff2 = true
    }
    if(isDiff1 && isDiff2) {
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
    snc.sql("drop schema if exists snappyDB")
  }
}
