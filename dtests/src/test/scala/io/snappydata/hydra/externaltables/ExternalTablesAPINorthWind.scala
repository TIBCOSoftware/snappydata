/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.hydra.externaltables

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class ExternalTablesAPINorthWind extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    //  scalastyle:off println
    println("Started the CSV -> JSON and CSV -> AVRO conversion using external tables...")
    val snc : SnappyContext = snappySession.sqlContext
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    val dataFileLocation = jobConfig.getString("dataFilesLocation")
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()

    snc.sql("CREATE SCHEMA NW;")

    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
       dataFileLocation, "employees.csv", "NW.Employees")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "categories.csv", "NW.Categories")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "customers.csv", "NW.Customers")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "employee-territories.csv", "NW.EmployeeTerritories")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "order-details.csv", "NW.OrderDetails")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "orders.csv", "NW.Orders")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "products.csv", "NW.Products")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "regions.csv", "NW.Regions")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "shippers.csv", "NW.Shippers")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "suppliers.csv", "NW.Suppliers")
    loadDataFromSourceAndRunSelectQueryThenDropTable(snc, "csv",
      dataFileLocation, "territories.csv", "NW.Territories")

    // Create DataFrame From External Tabels
    val orderdetailsDF : DataFrame = snc.createExternalTable("NW.OrderDetails",
      "csv", Map ("path"-> (dataFileLocation + "order-details.csv") , "BUCKETS"-> "8"))
      .toDF("OrderId", "PrdId", "UnitPrice", "Qty", "Discount")

    val ordersDF : DataFrame = snc.createExternalTable("NW.Orders", "csv",
      Map ("path"-> (dataFileLocation + "orders.csv") , "BUCKETS"->"8"))
      .toDF("OrderId", "CustId", "EId", "OrderDt", "ReqDt", "ShippedDt",
      "ShippedVia", "Freight", "ShipName", "ShipAddr", "ShipCity", "ShipRegion",
      "ShipPostalCode", "ShipCountry")

    val empDF : DataFrame = snc.createExternalTable( "NW.Employees", "csv",
      Map ("path"->(dataFileLocation + "employees.csv") , "BUCKETS"->"8"))
      .toDF("EmpID", "LastName", "FirstName", "Title", "TitleOfCourtsey",
            "BirthDt", "HireDt", "Addr", "City", "Region", "PostalCd", "Country",
            "HomePhone", "Extension", "Photo", "Notes", "ReportsTo", "Photopath")

    val categoriesDF : DataFrame = snc.createExternalTable("NW.Categories" , "csv",
      Map("path" -> (dataFileLocation + "categories.csv") , "BUCKETS" -> "8"))

    val customerDF : DataFrame = snc.createExternalTable("NW.Customers" , "csv",
      Map("path" -> (dataFileLocation + "customers.csv") , "BUCKETS" -> "8"))

    val EmpTerDF : DataFrame = snc.createExternalTable("NW.EmployeeTerritories" , "csv",
      Map("path" -> (dataFileLocation + "employee-territories.csv") , "BUCKETS" -> "8"))

    val productDF : DataFrame = snc.createExternalTable("NW.Products" , "csv",
      Map("path" -> (dataFileLocation + "products.csv") , "BUCKETS" -> "8"))

    val regionDF : DataFrame = snc.createExternalTable("NW.Regions" , "csv",
      Map("path" -> (dataFileLocation + "regions.csv") , "BUCKETS" -> "8"))

    val shipperDF : DataFrame = snc.createExternalTable("NW.Shippers" , "csv",
      Map("path" -> (dataFileLocation + "shippers.csv") , "BUCKETS" -> "8"))

    val supplierDF : DataFrame = snc.createExternalTable("NW.Suppliers", "csv" ,
      Map("path" -> (dataFileLocation + "suppliers.csv") , "BUCKETS" -> "8"))

    val terrDF : DataFrame = snc.createExternalTable("NW.Territories", "csv",
      Map("path" -> (dataFileLocation + "territories.csv") , "BUCKETS" -> "8"))

    // SELECT Title, FirstName, LastName FROM Employees WHERE Title = 'Sales Representative';
    val salesRep = empDF.select(("Title"), "FirstName", "LastName").filter(empDF("Title") === "Sales Representative")
    println("Q1 : " + salesRep.show())

    //  SELECT FirstName, LastName FROM Employees WHERE Title <> 'Sales Representative';
    //  TODO : Test the where(String) or filter("String) condition
    //  val titleOtherThanSalsRep = empDF.select("FirstName" , "LastName")
    // .where("Title =!= Sales Representative")
    val titleOtherThanSalsRep = empDF.select("FirstName" , "LastName").filter(empDF("Title") =!= "Sales Representative")
    println("Q2 : " + titleOtherThanSalsRep.show())

    // SELECT FirstName, LastName FROM Employees WHERE LastName >= 'N' ORDER BY LastName DESC;
    val empName = empDF.select( "FirstName", "LastName").where("LastName >= 'N'").orderBy("LastName")
    println("Q3 : " + empName.show())

    //  SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders WHERE Freight >= 500;
    val freightTotal = ordersDF("Freight") * 1.1
    val addFreightTotal = ordersDF.withColumn("FreightTotal", freightTotal)
    val freightgeq500 = addFreightTotal.select("OrderId", "Freight", "FreightTotal")
      .where(addFreightTotal("Freight").geq(500))
    println("Q4 : " + freightgeq500.show())

    //  SELECT SUM(Quantity) AS TotalUnits FROM Order_Details WHERE ProductID=3;
    import org.apache.spark.sql.functions._
    val totalUnits = orderdetailsDF.filter(orderdetailsDF("PrdId").equalTo(3))
      .agg(sum("Qty").alias("TotalUnits"))
    println("Q5 : " + totalUnits.show())

    snc.dropTable("NW.Employees")
    snc.dropTable("NW.Categories")
    snc.dropTable("NW.Customers")
    snc.dropTable("NW.EmployeeTerritories")
    snc.dropTable("NW.OrderDetails")
    snc.dropTable("NW.Orders")
    snc.dropTable("NW.Products")
    snc.dropTable("NW.Regions")
    snc.dropTable("NW.Shippers")
    snc.dropTable("NW.Suppliers")
    snc.dropTable("NW.Territories")

    snc.sql("DROP SCHEMA NW;")
    println("ExternalTablesAPINorthWind completed.....")
  }

  def loadDataFromSourceAndRunSelectQueryThenDropTable(snc : SnappyContext, format : String,
                                                       dataFileLocation : String, file : String,
                                                       tableName : String) : Unit = {
    println("Read the data from source and run queries..., " +
      "File path is : " + dataFileLocation + file)
    println()
    val df : DataFrame = snc.createExternalTable(tableName, (dataFileLocation + file), "csv")
    val data = df.select("*")
    println("SELECT * FROM : " + tableName + data.show())
    snc.dropTable(tableName, true)
  }
}
