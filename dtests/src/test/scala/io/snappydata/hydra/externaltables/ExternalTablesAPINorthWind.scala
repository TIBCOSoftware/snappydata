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
    val spark : SparkSession = SparkSession.builder().getOrCreate()

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
    val sncOrderDetailsDF : DataFrame = snc.createExternalTable("NW.OrderDetails",
      "csv", Map ("path"-> (dataFileLocation + "order-details.csv") ,
        "BUCKETS"-> "8", "header" -> "true", "inferSchema"->"true"))
      .toDF("OrderId", "PrdId", "UnitPrice", "Qty", "Discount")
    val sparkOrderDetailsDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "order-details.csv")

    val sncOrdersDF : DataFrame = snc.createExternalTable("NW.Orders", "csv",
      Map ("path"-> (dataFileLocation + "orders.csv") , "BUCKETS"->"8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("OrderId", "CustId", "EId", "OrderDt", "ReqDt", "ShippedDt",
      "ShippedVia", "Freight", "ShipName", "ShipAddr", "ShipCity", "ShipRegion",
      "ShipPostalCode", "ShipCountry")
    val sparkOrdersDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "orders.csv")

    val sncEmpDF : DataFrame = snc.createExternalTable( "NW.Employees", "csv",
      Map ("path"->(dataFileLocation + "employees.csv") , "BUCKETS"->"8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("EmpID", "LastName", "FirstName", "Title", "TitleOfCourtesy",
            "BirthDt", "HireDt", "Addr", "City", "Region", "PostalCd", "Country",
            "HomePhone", "Extension", "Photo", "Notes", "ReportsTo", "Photopath")
    val sparkEmpDF : DataFrame = spark.read.format( "csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "employees.csv")

    val sncCategoriesDF : DataFrame = snc.createExternalTable("NW.Categories" , "csv",
      Map("path" -> (dataFileLocation + "categories.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("CategoryID", "CategoryName", "Description", "Picture")
    val sparkCategoriesDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "categories.csv")

    val sncCustomerDF : DataFrame = snc.createExternalTable("NW.Customers" , "csv",
      Map("path" -> (dataFileLocation + "customers.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("CustID", "CompanyName", "ContactName", "ContactTitle", "Addr", "City",
        "Region", "PostalCode", "Country", "Phone", "Fax")
    val sparkCustomerDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "customers.csv")

    val sncEmpTerDF : DataFrame = snc.createExternalTable("NW.EmployeeTerritories" , "csv",
      Map("path" -> (dataFileLocation + "employee-territories.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("EmpID", "TerritoryID")
    val sparkEmpTerDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "employee-territories.csv")

    val sncProductDF : DataFrame = snc.createExternalTable("NW.Products" , "csv",
      Map("path" -> (dataFileLocation + "products.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("ProdID", "ProductName", "SupplierID", "CategoryID", "QtyPerUnit", "UnitPrice",
        "UnitsInStock", "UnitsOnOrder", "ReorderLevel", "Discontinued")
    val sparkProductDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "products.csv")

    val sncRegionDF : DataFrame = snc.createExternalTable("NW.Regions" , "csv",
      Map("path" -> (dataFileLocation + "regions.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("RegionID", "RegionDescription")
    val sparkRegionDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "regions.csv")

    val sncShipperDF : DataFrame = snc.createExternalTable("NW.Shippers" , "csv",
      Map("path" -> (dataFileLocation + "shippers.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("ShipperID", "CompanyName", "Phone")
    val sparkShipperDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "shippers.csv")

    val sncSupplierDF : DataFrame = snc.createExternalTable("NW.Suppliers", "csv" ,
      Map("path" -> (dataFileLocation + "suppliers.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("SupplierID", "CompanyName", "ContactName", "ContactTitle", "Addr", "City",
        "Region", "PostalCode", "Country", "Phone", "Fax", "HomePage")
    val sparkSupplierDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load(dataFileLocation + "suppliers.csv")

    val sncTerrDF : DataFrame = snc.createExternalTable("NW.Territories", "csv",
      Map("path" -> (dataFileLocation + "territories.csv") , "BUCKETS" -> "8",
        "header" -> "true", "inferSchema"->"true"))
      .toDF("TerritoryID", "TerritoryDescription", "RegionID")
    val sparkTerrDF : DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferScheam", "true")
      .load(dataFileLocation + "territories.csv")

    /* <1> SELECT TitleOfCourtesy, FirstName, LastName FROM Employees
           WHERE TitleOfCourtesy IN ('Ms.','Mrs.'); */
    val snc_namesWithTOC = sncEmpDF.select("TitleOfCourtesy", "FirstName", "LastName")
      .where(sncEmpDF("TitleOfCourtesy").isin("Ms.", "Mrs."))
    val spark_namesWithTOC = sparkEmpDF.select("TitleOfCourtesy", "FirstName", "LastName")
        .where(sparkEmpDF("TitleOfCourtesy").isin("Ms.", "Mrs."))
    println("snc_namesWithTOC : " + snc_namesWithTOC.show())
    println("spark_namesWithTOC : " + spark_namesWithTOC.show())
    println(snc_namesWithTOC.except(spark_namesWithTOC).show())


    /*  <2> SELECT FirstName, LastName FROM Employees; */
    val snc_names = sncEmpDF.select("FirstName" , "LastName")
    val spark_names = sparkEmpDF.select("FirstName", "LastName")
    println("snc_names : " + snc_names.show())
    println("spark_names : " + spark_names.show())
    println(snc_names.except(spark_names))

    /*  <3> SELECT FirstName, LastName FROM Employees ORDER BY LastName; */
    val snc_namesSortByLastName = sncEmpDF.select("FirstName", "LastName").orderBy(sncEmpDF("LastName").desc)
    val spark_namesSortByLastName = sparkEmpDF.select("FirstName", "LastName")
      .orderBy(sparkEmpDF("LastName").desc)
    println("snc_namesSortByLastName : " + snc_namesSortByLastName.show())
    println("spark_namesSortByLastName : " + spark_namesSortByLastName.show())
    println(snc_namesSortByLastName.except(spark_namesSortByLastName))


    /* <4> SELECT Title, FirstName, LastName FROM Employees WHERE Title = 'Sales Representative'; */
    val snc_salesRep = sncEmpDF.select(("Title"), "FirstName", "LastName").filter(sncEmpDF("Title") === "Sales Representative")
    val spark_SalesRep = sparkEmpDF.select("Title", "FirstName", "LastName")
        .filter((sparkEmpDF("Title") === "Sales Representative"))
    println("snc_salesRep : " + snc_salesRep.show())
    println("spark_salesRep : " + spark_SalesRep.show())
    println(snc_salesRep.except(spark_SalesRep))

    /*  <5> SELECT FirstName, LastName FROM Employees WHERE Title <> 'Sales Representative';
    //  TODO : Test the where(String) or filter("String) condition
    //  val snc_titleOtherThanSalsRep = sncEmpDF.select("FirstName" , "LastName")
    // .where("Title =!= Sales Representative") */
    val snc_titleOtherThanSalsRep = sncEmpDF.select("FirstName" , "LastName").filter(sncEmpDF("Title") =!= "Sales Representative")
    val spark_titleOtherThanSalsRep = sparkEmpDF.select("FirstName", "LastName")
        .filter(sparkEmpDF("Title") =!= "Sales Representative")
    println("snc_titleOtherThanSalsRep : " + snc_titleOtherThanSalsRep.show())
    println("spark_titleOtherThanSalsRep : " + spark_titleOtherThanSalsRep.show())
    println(snc_titleOtherThanSalsRep.except(spark_titleOtherThanSalsRep))

    /* <6> SELECT FirstName, LastName FROM Employees WHERE LastName >= 'N'
           ORDER BY LastName DESC; */
    val snc_EmpName = sncEmpDF.select( "FirstName", "LastName")
      .where("LastName >= 'N'").orderBy("LastName")
    val spark_EmpName = sparkEmpDF.select("FirstName", "LastName")
        .where("LastName >= 'N'").orderBy("LastName")
    println("snc_empName : " + snc_EmpName.show())
    println("spark_empName : " + spark_EmpName.show())
    println(snc_EmpName.except(spark_EmpName))

    /*  <7> SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders
    WHERE Freight >= 500; */
    val snc_FreightTotal = sncOrdersDF("Freight") * 1.1
    val snc_AddFreightTotal = sncOrdersDF.withColumn("FreightTotal", snc_FreightTotal)
    val snc_Freightgeq500 = snc_AddFreightTotal.select("OrderId", "Freight", "FreightTotal")
      .where(snc_AddFreightTotal("Freight").geq(500))
    val spark_FreightTotal = sparkOrdersDF("Freight") * 1.1
    val spark_AddFreightTotal = sparkOrdersDF.withColumn("FreightTotal", spark_FreightTotal)
    val spark_Freightgeq500 = spark_AddFreightTotal.select("OrderID", "Freight" , "FreightTotal")
        .where(spark_AddFreightTotal("Freight").geq(500))
    println("snc_freightgeq500 : " + snc_Freightgeq500.show())
    println("spark_freightgeq500 : " + spark_Freightgeq500.show())
    println(snc_Freightgeq500.except(spark_Freightgeq500))

    /* <8> SELECT SUM(Quantity) AS TotalUnits FROM Order_Details WHERE ProductID=3; */
    import org.apache.spark.sql.functions._
    val snc_TotalUnits = sncOrderDetailsDF.filter(sncOrderDetailsDF("PrdId").equalTo(3))
      .agg(sum("Qty").alias("TotalUnits"))
    val spark_TotalUnits = sparkOrderDetailsDF.filter(sparkOrderDetailsDF("ProductID").equalTo(3))
        .agg(sum("Quantity").alias("TotalUnits"))
    println("snc_totalUnits : " + snc_TotalUnits.show())
    println("spark_totalUnits : " + spark_TotalUnits.show())

    /* <9> SELECT COUNT(DISTINCT City) AS NumCities FROM Employees; */
    val snc_DistinctCityCount = sncEmpDF.select("City").distinct().alias("NumCities")
    val spark_DistinctCityCount = sparkEmpDF.select("City").distinct().alias("NumCities")
    println("snc_DistinctCityCount : " + snc_DistinctCityCount.show())
    println("spark_DistinctCityCount : " + spark_DistinctCityCount.show())
    println(snc_DistinctCityCount.except(spark_DistinctCityCount))


    /* Will add all the NorthWind Queries */

    /* <10> SELECT FirstName, LastName, BirthDate FROM Employees WHERE BirthDate
            BETWEEN Cast('1950-01-01' as TIMESTAMP) AND Cast('1959-12-31 23:59:59' as TIMESTAMP); */
//    val snc_EmpBirthRec = sncEmpDF.select("FirstName", "LastName", "BirthDt")
//        .where(sncEmpDF("BirthDt").between("1950-01-01", "1959-12-31 23:59:59"))


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
