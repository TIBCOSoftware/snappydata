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
package org.apache.spark.sql

import io.snappydata.SnappyFunSuite
import org.apache.spark.Logging
import org.apache.spark.sql.execution.aggregate.{SortBasedAggregate, TungstenAggregate}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.{LocalTableScan, PartitionedPhysicalRDD, Project, Sort}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class NorthWindTest
  extends SnappyFunSuite
  with Logging
  with BeforeAndAfter
  with BeforeAndAfterAll {

  after {
   dropTables(snc)
  }

  test("Test replicated row tables queries") {
    createAndLoadReplicatedTables(snc)
    validateReplicatedTableQueries(snc)
  }

  test("Test partitioned row tables queries") {
    createAndLoadPartitionedTables(snc)
    validatePartitionedRowTableQueries(snc)
  }

  test("Test column tables queries") {
    createAndLoadColumnTables(snc)
    validatePartitionedColumnTableQueries(snc)
  }

  test("Test colocated tables queries") {
    createAndLoadColocatedTables(snc)
    validateColocatedTableQueries(snc)
  }

  private def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int,
                         numPartitions: Int, c: Class[_]): Any = {
    val df = snc.sql(sqlString)
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: LocalJoin => j
      case j: LeftSemiJoinHash => j
      case j: BroadcastHashJoin => j
      case j: BroadcastHashOuterJoin => j
      case j: BroadcastNestedLoopJoin => j
      case j: BroadcastLeftSemiJoinHash => j
      case j: LeftSemiJoinBNL => j
      case j: CartesianProduct => j
      case j: SortMergeJoin => j
      case j: SortMergeOuterJoin => j
    }
    if (operators(0).getClass() != c) {
      throw new IllegalStateException(s"$sqlString expected operator: $c," +
        s" but got ${operators(0)}\n physical: \n$physical")
    }
    assert(df.count() == numRows,
      "Mismatch got df.count ->" + df.count() + " but expected numRows ->" + numRows)
    assert(df.rdd.partitions.length == numPartitions,
      "Mismatch got df.rdd.partitions.length ->" + df.rdd.partitions.length +
        " but expected numPartitions ->" + numPartitions)
  }

  private def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int,
                          numPartitions: Int, c: Class[_]): Any = {
    val df = snc.sql(sqlString)
    df.explain()
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: SortBasedAggregate => j
      case j: Sort => j
      case j: Project => j
      case j: TungstenAggregate => j
      case j: PartitionedPhysicalRDD => j
      case j: LocalTableScan => j
    }
    if (operators(0).getClass() != c) {
      throw new IllegalStateException(s"$sqlString expected operator: $c," +
        s" but got ${operators(0)}\n physical: \n$physical")
    }
    assert(df.count() == numRows,
      "Mismatch got df.count ->" + df.count() + " but expected numRows ->" + numRows)

    assert(df.rdd.partitions.length == numPartitions,
      "Mismatch got df.rdd.partitions.length ->" + df.rdd.partitions.length +
        " but expected numPartitions ->" + numPartitions)
  }

  private def createAndLoadColocatedTables(snc: SnappyContext): Unit = {

    val regions = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/regions.csv").getPath)
    snc.sql("create table regions (" +
      "RegionID int, " +
      "RegionDescription string)")
    regions.write.insertInto("regions")
    assert(snc.sql("select * from regions").count() == 4)

    val categories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/categories.csv").getPath)
    snc.sql("create table categories (" +
      "CategoryID int, " +
      "CategoryName string, " +
      "Description string, " +
      "Picture blob)")
    categories.write.insertInto("categories")
    assert(snc.sql("select * from categories").count() == 8)

    val shippers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/shippers.csv").getPath)
    snc.sql("create table shippers (" +
      "ShipperID int not null, " +
      "CompanyName string not null, " +
      "Phone string)")
    shippers.write.insertInto("shippers")
    assert(snc.sql("select * from shippers").count() == 3)

    val employees = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employees.csv").getPath)
    snc.sql("create table employees(" +
      "EmployeeID int, " +
      "LastName string, " +
      "FirstName string, " +
      "Title string, " +
      "TitleOfCourtesy string, " +
      "BirthDate timestamp, " +
      "HireDate timestamp, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "HomePhone string, " +
      "Extension string, " +
      "Photo blob, " +
      "Notes string, " +
      "ReportsTo int, " +
      "PhotoPath string) using row options( partition_by 'EmployeeID', buckets '3')")
    employees.write.insertInto("employees")
    assert(snc.sql("select * from employees").count() == 8)

    val customers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/customers.csv").getPath)
    snc.sql("create table customers(" +
      "CustomerID string, " +
      "CompanyName string, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string) " +
      "using column options( partition_by 'CustomerID', buckets '19')")
    customers.write.insertInto("customers")
    assert(snc.sql("select * from customers").count() == 91)

    val orders = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/orders.csv").getPath)
    snc.sql("create table orders (" +
      "OrderID int, " +
      "CustomerID string, " +
      "EmployeeID int, " +
      "OrderDate timestamp, " +
      "RequiredDate timestamp, " +
      "ShippedDate timestamp, " +
      "ShipVia int, " +
      "Freight double, " +
      "ShipName string, " +
      "ShipAddress string, " +
      "ShipCity string, " +
      "ShipRegion string, " +
      "ShipPostalCode string, " +
      "ShipCountry string) using row options (partition_by 'CustomerID', " +
      "buckets '19', colocate_with 'customers')")
    orders.write.insertInto("orders")
    assert(snc.sql("select * from orders").count() == 830)

    val order_details = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/order-details.csv").getPath)
    snc.sql("create table order_details (" +
      "OrderID int, " +
      "ProductID int, " +
      "UnitPrice double, " +
      "Quantity int, " +
      "Discount double) using column options (" +
      " partition_by 'ProductID', buckets '329')")
    order_details.write.insertInto("order_details")
    assert(snc.sql("select * from order_details").count() == 2155)

    val products = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/products.csv").getPath)
    snc.sql("create table products(" +
      "ProductID int, " +
      "ProductName string, " +
      "SupplierID int, " +
      "CategoryID int," +
      "QuantityPerUnit string, " +
      "UnitPrice double, " +
      "UnitsInStock int, " +
      "UnitsOnOrder int," +
      "ReorderLevel int, " +
      "Discontinued int) USING row options ( partition_by 'ProductID', buckets '329'," +
      "colocate_with 'order_details')")
    products.write.insertInto("products")
    assert(snc.sql("select * from products").count() == 77)

    val suppliers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/suppliers.csv").getPath)
    snc.sql("create table suppliers(" +
      "SupplierID int, " +
      "CompanyName string, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string, " +
      "HomePage string) USING column options (PARTITION_BY 'SupplierID'," +
      " buckets '123')")
    suppliers.write.insertInto("suppliers")
    assert(snc.sql("select * from suppliers").count() == 29)

    val territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/territories.csv").getPath)
    snc.sql("create table territories(" +
      "TerritoryID string , " +
      "TerritoryDescription string, " +
      "RegionID string) using column options (partition_by 'TerritoryID', buckets '3')")
    territories.write.insertInto("territories")
    assert(snc.sql("select * from territories").count() == 53)

    val employee_territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employee-territories.csv").getPath)
    snc.sql("create table employee_territories(" +
      "EmployeeID int, " +
      "TerritoryID string) using row options(partition_by 'TerritoryID'," +
      " buckets '3', colocate_with 'territories') ")
    employee_territories.write.insertInto("employee_territories")
    assert(snc.sql("select * from employee_territories").count() == 49)

  }
  private def validatePartitionedColumnTableQueries(snc: SnappyContext): Unit = {

    // Exploring the Tables
    assertQuery(snc, "SELECT * FROM Categories"
      , 8, 1, classOf[PartitionedPhysicalRDD])


    assertQuery(snc, "SELECT * FROM Customers"
      , 91, 1, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT * FROM Orders"
      , 830, 4, classOf[PartitionedPhysicalRDD])

    // SELECTing Specific Columns
    assertQuery(snc, "SELECT FirstName, LastName FROM Employees"
      , 8, 4, classOf[PartitionedPhysicalRDD])

    // Sorting By Multiple Columns
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY LastName", 8, 9, classOf[Sort])

    // Sorting By Column Position
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY 1,3", 8, 2, classOf[Sort])

    // Ascending and Descending Sorts
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " ORDER BY Title ASC, LastName DESC", 8, 9, classOf[Sort])

    // Checking for Equality
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'", 5, 4, classOf[PartitionedPhysicalRDD])

    // Checking for Inequality
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE Title <> 'Sales Representative'", 3, 4, classOf[Project])

    // Checking for Greater or Less Than
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE LastName >= 'N'", 2, 4, classOf[PartitionedPhysicalRDD])

    // Checking for NULL
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE Region IS NULL", 0, 4, classOf[Project])

    // WHERE and ORDER BY
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE LastName >= 'N'" +
      " ORDER BY LastName DESC", 2, 3, classOf[Sort])

    // Using the WHERE clause to check for equality or inequality
    assertQuery(snc, "SELECT OrderDate, ShippedDate, CustomerID, Freight" +
      " FROM Orders " +
      " WHERE OrderDate = '19-May-1997'", 0, 4, classOf[LocalTableScan])

    // Using WHERE and ORDER BY Together
    assertQuery(snc, "SELECT CompanyName, ContactName, Fax" +
      " FROM Customers" +
      " WHERE Fax IS NOT NULL" +
      " ORDER BY CompanyName", 91, 1, classOf[Sort])

    // The IN Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy IN ('Ms.','Mrs.')", 5, 4, classOf[PartitionedPhysicalRDD])

    // The LIKE Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy LIKE 'M%'", 7, 4, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT FirstName, LastName, BirthDate" +
      " FROM Employees" +
      " WHERE BirthDate BETWEEN '1950-01-01' AND '1959-12-31 23:59:59'",
      3, 4, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT CONCAT(FirstName, ' ', LastName)" +
      " FROM Employees", 8, 4, classOf[Project])

    assertQuery(snc, "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal" +
      " FROM Orders" +
      " WHERE Freight >= 500", 13, 4, classOf[Project])

    assertQuery(snc, "SELECT SUM(Quantity) AS TotalUnits" +
      " FROM Order_Details" +
      " WHERE ProductID=3", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT MIN(HireDate) AS FirstHireDate," +
      " MAX(HireDate) AS LastHireDate" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT City, COUNT(EmployeeID) AS NumEmployees" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'" +
      " GROUP BY City" +
      " HAVING COUNT(EmployeeID) > 1" +
      " ORDER BY NumEmployees", 1, 2, classOf[Sort])

    assertQuery(snc, "SELECT COUNT(DISTINCT City) AS NumCities" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT ProductID, AVG(UnitPrice) AS AveragePrice" +
      " FROM Products " +
      " GROUP BY ProductID " +
      " HAVING AVG(UnitPrice) > 70" +
      " ORDER BY AveragePrice", 4, 5, classOf[Sort])

    //        assertJoin(snc, "SELECT CompanyName FROM Customers WHERE CustomerID = " +
    //          "(SELECT CustomerID FROM Orders WHERE OrderID = 10290)"
    //          , 1, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftHashJOin

    //        assertJoin(snc, "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
    //          "FROM Orders WHERE OrderDate BETWEEN '1-Jan-1997' AND '31-Dec-1997')"
    //          , 89, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftSemiJoinHash

    //        assertJoin(snc, "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    //          " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    //          "('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'))"
    //          , 9, 4, classOf[BroadcastLeftSemiJoinHash]) // LeftSemiJoinHash

//    assertJoin(snc, "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
//      "CategoryID FROM Categories WHERE CategoryName = 'Seafood')"
//      , 12, 200, classOf[LeftSemiJoinHash])

    //        assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
    //          "(SELECT SupplierID FROM Products WHERE CategoryID = 8)"
    //          , 8, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftSemiJoinHash

//    assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
//      " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
//      " WHERE CategoryName = 'Seafood'))" , 8, 200, classOf[LeftSemiJoinHash])

    assertJoin(snc, "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " ORDER BY Orders.OrderDate" , 758, 200, classOf[BroadcastHashJoin])


    //        assertJoin(snc, "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName" +
    //          " FROM Orders o" +
    //          " JOIN Employees e ON (e.EmployeeID = o.EmployeeID)" +
    //          " JOIN Customers c ON (c.CustomerID = o.CustomerID)" +
    //          " WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1-Jan-1998'" +
    //          " ORDER BY c.CompanyName" , 51, 1, classOf[LocalJoin])


    //        assertJoin(snc, "SELECT e.FirstName, e.LastName, o.OrderID" +
    //          " FROM Employees e JOIN Orders o ON" +
    //          " (e.EmployeeID = o.EmployeeID)" +
    //          " WHERE o.RequiredDate < o.ShippedDate" +
    //          " ORDER BY e.LastName, e.FirstName" , 51, 1 , classOf[LocalJoin])

    //    assertJoin(snc, "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
    //      " FROM Order_Details od JOIN Products p ON" +
    //      " (p.ProductID = od.ProductID)" +
    //      " GROUP BY p.ProductName" +
    //      " HAVING SUM(Quantity) < 200" , 5, 200, classOf[SortMergeJoin]) // BroadcastHashJoin

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e JOIN Customers c ON" +
      " (e.City = c.City)" +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC", 3, 4, classOf[LocalJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e LEFT JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC" , 5, 4, classOf[SortMergeOuterJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City " +
      " FROM Employees e RIGHT JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City" +
      " ORDER BY numEmployees DESC" , 69, 5, classOf[BroadcastHashOuterJoin]) // SortMergeOuterJoin

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e FULL JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC" , 71, 5, classOf[SortMergeOuterJoin])

    assertJoin(snc, "select s.supplierid,s.companyname,p.productid,p.productname " +
      "from suppliers s join products p on(s.supplierid= p.supplierid) and" +
      " s.companyname IN('Grandma Kelly''s Homestead','Tokyo Traders','Exotic Liquids')"
      , 9, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID"
      , 830, 4, classOf[LocalJoin])

    assertJoin(snc, "SELECT order_details.OrderID,ShipCountry,UnitPrice,Quantity,Discount" +
      " FROM orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry"
      , 22, 200, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId"
      , 830, 4, classOf[BroadcastLeftSemiJoinHash])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details"
      , 830, 4, classOf[LeftSemiJoinBNL])

    assertJoin(snc, "SELECT * FROM orders JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])

    //    assertJoin(snc, "SELECT * FROM orders JOIN order_details" +
    //      " ON Orders.OrderID = Order_Details.OrderID"
    //      , 2155, 4, classOf[SortMergeJoin]) // SortMergeJoin  //BroadcastHashJoin

    //    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details" +
    //      " ON Orders.OrderID = Order_Details.OrderID"
    //      , 2155, 4, classOf[SortMergeOuterJoin]) //BroadcastHashOuterJoin
    //    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details" +
    //      " ON Orders.OrderID = Order_Details.OrderID"
    //      , 2155, 4, classOf[SortMergeOuterJoin]) // BroadcastHashOuterJoin
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[SortMergeOuterJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[SortMergeOuterJoin])
  }

  private def validateColocatedTableQueries(snc: SnappyContext): Unit = {

    // Exploring the Tables
    assertQuery(snc, "SELECT * FROM Categories"
      , 8, 1, classOf[PartitionedPhysicalRDD])


    assertQuery(snc, "SELECT * FROM Customers"
      , 91, 4, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT * FROM Orders"
      , 830, 4, classOf[PartitionedPhysicalRDD])

    // SELECTing Specific Columns
    assertQuery(snc, "SELECT FirstName, LastName FROM Employees"
      , 8, 3, classOf[PartitionedPhysicalRDD])

    // Sorting By Multiple Columns
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY LastName", 8, 9, classOf[Sort])

    // Sorting By Column Position
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY 1,3", 8, 2, classOf[Sort])

    // Ascending and Descending Sorts
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " ORDER BY Title ASC, LastName DESC", 8, 9, classOf[Sort])

    // Checking for Equality
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'", 5, 3, classOf[PartitionedPhysicalRDD])

    // Checking for Inequality
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE Title <> 'Sales Representative'", 3, 3, classOf[Project])

    // Checking for Greater or Less Than
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE LastName >= 'N'", 2, 3, classOf[PartitionedPhysicalRDD])

    // Checking for NULL
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE Region IS NULL", 0, 3, classOf[Project])

    // WHERE and ORDER BY
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE LastName >= 'N'" +
      " ORDER BY LastName DESC", 2, 3, classOf[Sort])

    // Using the WHERE clause to check for equality or inequality
    assertQuery(snc, "SELECT OrderDate, ShippedDate, CustomerID, Freight" +
      " FROM Orders " +
      " WHERE OrderDate = '19-May-1997'", 0, 4, classOf[LocalTableScan])

    // Using WHERE and ORDER BY Together
    assertQuery(snc, "SELECT CompanyName, ContactName, Fax" +
      " FROM Customers" +
      " WHERE Fax IS NOT NULL" +
      " ORDER BY CompanyName", 91, 92, classOf[Sort])

    // The IN Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy IN ('Ms.','Mrs.')", 5, 3, classOf[PartitionedPhysicalRDD])

    // The LIKE Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy LIKE 'M%'", 7, 3, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT FirstName, LastName, BirthDate" +
      " FROM Employees" +
      " WHERE BirthDate BETWEEN '1950-01-01' AND '1959-12-31 23:59:59'",
      3, 3, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT CONCAT(FirstName, ' ', LastName)" +
      " FROM Employees", 8, 3, classOf[Project])

    assertQuery(snc, "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal" +
      " FROM Orders" +
      " WHERE Freight >= 500", 13, 4, classOf[Project])

    assertQuery(snc, "SELECT SUM(Quantity) AS TotalUnits" +
      " FROM Order_Details" +
      " WHERE ProductID=3", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT MIN(HireDate) AS FirstHireDate," +
      " MAX(HireDate) AS LastHireDate" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT City, COUNT(EmployeeID) AS NumEmployees" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'" +
      " GROUP BY City" +
      " HAVING COUNT(EmployeeID) > 1" +
      " ORDER BY NumEmployees", 1, 2, classOf[Sort])

    assertQuery(snc, "SELECT COUNT(DISTINCT City) AS NumCities" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT ProductID, AVG(UnitPrice) AS AveragePrice" +
      " FROM Products " +
      " GROUP BY ProductID " +
      " HAVING AVG(UnitPrice) > 70" +
      " ORDER BY AveragePrice", 4, 5, classOf[Sort])

    //        assertJoin(snc, "SELECT CompanyName FROM Customers WHERE CustomerID = " +
    //          "(SELECT CustomerID FROM Orders WHERE OrderID = 10290)"
    //          , 1, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftHashJOin

    //        assertJoin(snc, "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
    //          "FROM Orders WHERE OrderDate BETWEEN '1-Jan-1997' AND '31-Dec-1997')"
    //          , 89, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftSemiJoinHash

    //        assertJoin(snc, "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    //          " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    //          "('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'))"
    //          , 9, 4, classOf[BroadcastLeftSemiJoinHash]) // LeftSemiJoinHash

//    assertJoin(snc, "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
//      "CategoryID FROM Categories WHERE CategoryName = 'Seafood')"
//      , 12, 200, classOf[LeftSemiJoinHash])

    //        assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
    //          "(SELECT SupplierID FROM Products WHERE CategoryID = 8)"
    //          , 8, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftSemiJoinHash

    //    assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
    //      " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
    //      " WHERE CategoryName = 'Seafood'))" , 8, 200, classOf[LeftSemiJoinHash])
    //
    //    assertJoin(snc, "SELECT Employees.EmployeeID, Employees.FirstName," +
    //      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
    //      " FROM Employees JOIN Orders ON" +
    //      " (Employees.EmployeeID = Orders.EmployeeID)" +
    //      " ORDER BY Orders.OrderDate" , 758, 200, classOf[BroadcastHashJoin])


    //        assertJoin(snc, "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName" +
    //          " FROM Orders o" +
    //          " JOIN Employees e ON (e.EmployeeID = o.EmployeeID)" +
    //          " JOIN Customers c ON (c.CustomerID = o.CustomerID)" +
    //          " WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1-Jan-1998'" +
    //          " ORDER BY c.CompanyName" , 51, 1, classOf[LocalJoin])


    //        assertJoin(snc, "SELECT e.FirstName, e.LastName, o.OrderID" +
    //          " FROM Employees e JOIN Orders o ON" +
    //          " (e.EmployeeID = o.EmployeeID)" +
    //          " WHERE o.RequiredDate < o.ShippedDate" +
    //          " ORDER BY e.LastName, e.FirstName" , 51, 1 , classOf[LocalJoin])

    //    assertJoin(snc, "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
    //      " FROM Order_Details od JOIN Products p ON" +
    //      " (p.ProductID = od.ProductID)" +
    //      " GROUP BY p.ProductName" +
    //      " HAVING SUM(Quantity) < 200" , 5, 200, classOf[SortMergeJoin]) // BroadcastHashJoin

    //    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
    //      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
    //      " e.City, c.City" +
    //      " FROM Employees e JOIN Customers c ON" +
    //      " (e.City = c.City)" +
    //      " GROUP BY e.City, c.City " +
    //      " ORDER BY numEmployees DESC", 3, 4, classOf[BroadcastHashJoin]) //SortMergeJoin

    //    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
    //      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
    //      " e.City, c.City" +
    //      " FROM Employees e LEFT JOIN Customers c ON" +
    //      " (e.City = c.City) " +
    //      " GROUP BY e.City, c.City " +
    //      " ORDER BY numEmployees DESC" , 5, 4, classOf[BroadcastHashOuterJoin]) // SortMergeOuterJoin
    //
    //    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
    //      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
    //      " e.City, c.City " +
    //      " FROM Employees e RIGHT JOIN Customers c ON" +
    //      " (e.City = c.City) " +
    //      " GROUP BY e.City, c.City" +
    //      " ORDER BY numEmployees DESC" , 69, 5, classOf[BroadcastHashOuterJoin]) // SortMergeOuterJoin

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e FULL JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC" , 71, 5, classOf[SortMergeOuterJoin])

    assertJoin(snc, "select s.supplierid,s.companyname,p.productid,p.productname " +
      "from suppliers s join products p on(s.supplierid= p.supplierid) and" +
      " s.companyname IN('Grandma Kelly''s Homestead','Tokyo Traders','Exotic Liquids')"
      , 9, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID"
      , 830, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT order_details.OrderID,ShipCountry,UnitPrice,Quantity,Discount" +
      " FROM orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry"
      , 22, 200, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId"
      , 830, 4, classOf[BroadcastLeftSemiJoinHash])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details"
      , 830, 4, classOf[LeftSemiJoinBNL])

    assertJoin(snc, "SELECT * FROM orders JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    //
    //    //    assertJoin(snc, "SELECT * FROM orders JOIN order_details" +
    //    //      " ON Orders.OrderID = Order_Details.OrderID"
    //    //      , 2155, 4, classOf[SortMergeJoin]) // SortMergeJoin  //BroadcastHashJoin
    //
    //    //    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details" +
    //    //      " ON Orders.OrderID = Order_Details.OrderID"
    //    //      , 2155, 4, classOf[SortMergeOuterJoin]) //BroadcastHashOuterJoin
    //    //    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details" +
    //    //      " ON Orders.OrderID = Order_Details.OrderID"
    //    //      , 2155, 4, classOf[SortMergeOuterJoin]) // BroadcastHashOuterJoin
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 200, classOf[SortMergeOuterJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 200, classOf[SortMergeOuterJoin])
  }
  private def validatePartitionedRowTableQueries(snc: SnappyContext): Unit = {

    // Exploring the Tables
    assertQuery(snc, "SELECT * FROM Categories"
      , 8, 1, classOf[PartitionedPhysicalRDD])


    assertQuery(snc, "SELECT * FROM Customers"
      , 91, 1, classOf[PartitionedPhysicalRDD])

    //        assertQuery(snc, "SELECT * FROM Orders"
    //          , 830, 4, classOf[PartitionedPhysicalRDD])  got df.rdd.partitions.length ->1

    // SELECTing Specific Columns
    assertQuery(snc, "SELECT FirstName, LastName FROM Employees"
      , 8, 1, classOf[PartitionedPhysicalRDD])

    // Sorting By Multiple Columns
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY LastName", 8, 1, classOf[Sort])

    // Sorting By Column Position
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY 1,3", 8, 1, classOf[Sort])

    // Ascending and Descending Sorts
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " ORDER BY Title ASC, LastName DESC", 8, 1, classOf[Sort])

    // Checking for Equality
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'", 5, 1, classOf[PartitionedPhysicalRDD])

    // Checking for Inequality
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE Title <> 'Sales Representative'", 3, 1, classOf[Project])

    // Checking for Greater or Less Than
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE LastName >= 'N'", 2, 1, classOf[PartitionedPhysicalRDD])

    // Checking for NULL
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE Region IS NULL", 0, 1, classOf[Project])

    // WHERE and ORDER BY
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE LastName >= 'N'" +
      " ORDER BY LastName DESC", 2, 1, classOf[Sort])

    //        // Using the WHERE clause to check for equality or inequality
    //        assertQuery(snc, "SELECT OrderDate, ShippedDate, CustomerID, Freight" +
    //          " FROM Orders " +
    //          " WHERE OrderDate = '19-May-1997'", 0, 4, classOf[LocalTableScan]) //got df.rdd.partitions.length ->12

    // Using WHERE and ORDER BY Together
    assertQuery(snc, "SELECT CompanyName, ContactName, Fax" +
      " FROM Customers" +
      " WHERE Fax IS NOT NULL" +
      " ORDER BY CompanyName", 91, 1, classOf[Sort])

    // The IN Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy IN ('Ms.','Mrs.')", 5, 1, classOf[PartitionedPhysicalRDD])

    // The LIKE Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy LIKE 'M%'", 7, 1, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT FirstName, LastName, BirthDate" +
      " FROM Employees" +
      " WHERE BirthDate BETWEEN '1950-01-01' AND '1959-12-31 23:59:59'",
      3, 1, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT CONCAT(FirstName, ' ', LastName)" +
      " FROM Employees", 8, 1, classOf[Project])

    //        assertQuery(snc, "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal" +
    //          " FROM Orders" +
    //          " WHERE Freight >= 500", 13, 4, classOf[Project]) //got df.rdd.partitions.length ->1

    assertQuery(snc, "SELECT SUM(Quantity) AS TotalUnits" +
      " FROM Order_Details" +
      " WHERE ProductID=3", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT MIN(HireDate) AS FirstHireDate," +
      " MAX(HireDate) AS LastHireDate" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT City, COUNT(EmployeeID) AS NumEmployees" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'" +
      " GROUP BY City" +
      " HAVING COUNT(EmployeeID) > 1" +
      " ORDER BY NumEmployees", 1, 1, classOf[Sort])

    assertQuery(snc, "SELECT COUNT(DISTINCT City) AS NumCities" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT ProductID, AVG(UnitPrice) AS AveragePrice" +
      " FROM Products " +
      " GROUP BY ProductID " +
      " HAVING AVG(UnitPrice) > 70" +
      " ORDER BY AveragePrice", 4, 5, classOf[Sort])

    //        assertJoin(snc, "SELECT CompanyName FROM Customers WHERE CustomerID = " +
    //          "(SELECT CustomerID FROM Orders WHERE OrderID = 10290)"
    //          , 1, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftHashJOin

    //        assertJoin(snc, "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
    //          "FROM Orders WHERE OrderDate BETWEEN '1-Jan-1997' AND '31-Dec-1997')"
    //          , 89, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftSemiJoinHash

    //        assertJoin(snc, "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    //          " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    //          "('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'))"
    //          , 9, 4, classOf[BroadcastLeftSemiJoinHash]) // LeftSemiJoinHash

//    assertJoin(snc, "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
//      "CategoryID FROM Categories WHERE CategoryName = 'Seafood')"
//      , 12, 200, classOf[LeftSemiJoinHash])

    //        assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
    //          "(SELECT SupplierID FROM Products WHERE CategoryID = 8)"
    //          , 8, 200, classOf[LeftSemiJoinHash]) // BroadcastLeftSemiJoinHash

//    assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
//      " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
//      " WHERE CategoryName = 'Seafood'))" , 8, 200, classOf[LeftSemiJoinHash])

    assertJoin(snc, "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " ORDER BY Orders.OrderDate" , 758, 200, classOf[LocalJoin])


    //        assertJoin(snc, "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName" +
    //          " FROM Orders o" +
    //          " JOIN Employees e ON (e.EmployeeID = o.EmployeeID)" +
    //          " JOIN Customers c ON (c.CustomerID = o.CustomerID)" +
    //          " WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1-Jan-1998'" +
    //          " ORDER BY c.CompanyName" , 51, 1, classOf[LocalJoin])


    //        assertJoin(snc, "SELECT e.FirstName, e.LastName, o.OrderID" +
    //          " FROM Employees e JOIN Orders o ON" +
    //          " (e.EmployeeID = o.EmployeeID)" +
    //          " WHERE o.RequiredDate < o.ShippedDate" +
    //          " ORDER BY e.LastName, e.FirstName" , 51, 1 , classOf[LocalJoin])

    //    assertJoin(snc, "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
    //      " FROM Order_Details od JOIN Products p ON" +
    //      " (p.ProductID = od.ProductID)" +
    //      " GROUP BY p.ProductName" +
    //      " HAVING SUM(Quantity) < 200" , 5, 200, classOf[SortMergeJoin]) // BroadcastHashJoin

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e JOIN Customers c ON" +
      " (e.City = c.City)" +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC", 3, 4, classOf[LocalJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e LEFT JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC" , 5, 4, classOf[SortMergeOuterJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City " +
      " FROM Employees e RIGHT JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City" +
      " ORDER BY numEmployees DESC" , 69, 5, classOf[SortMergeOuterJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e FULL JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC" , 71, 5, classOf[SortMergeOuterJoin])

    assertJoin(snc, "select s.supplierid,s.companyname,p.productid,p.productname " +
      "from suppliers s join products p on(s.supplierid= p.supplierid) and" +
      " s.companyname IN('Grandma Kelly''s Homestead','Tokyo Traders','Exotic Liquids')"
      , 9, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID"
      , 830, 4, classOf[LocalJoin])

    assertJoin(snc, "SELECT order_details.OrderID,ShipCountry,UnitPrice,Quantity,Discount" +
      " FROM orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry"
      , 22, 200, classOf[BroadcastHashJoin])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId"
      , 830, 4, classOf[BroadcastLeftSemiJoinHash])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details"
      , 830, 4, classOf[LeftSemiJoinBNL])

    assertJoin(snc, "SELECT * FROM orders JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details"
      , 1788650, 8, classOf[BroadcastNestedLoopJoin])

    //    assertJoin(snc, "SELECT * FROM orders JOIN order_details" +
    //      " ON Orders.OrderID = Order_Details.OrderID"
    //      , 2155, 4, classOf[SortMergeJoin]) // SortMergeJoin  //BroadcastHashJoin

    //    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details" +
    //      " ON Orders.OrderID = Order_Details.OrderID"
    //      , 2155, 4, classOf[SortMergeOuterJoin]) //BroadcastHashOuterJoin
    //    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details" +
    //      " ON Orders.OrderID = Order_Details.OrderID"
    //      , 2155, 4, classOf[SortMergeOuterJoin]) // BroadcastHashOuterJoin
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[SortMergeOuterJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 4, classOf[SortMergeOuterJoin])
  }
  private def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

    val regions = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/regions.csv").getPath)
    snc.sql("create table regions (" +
      "RegionID int, " +
      "RegionDescription string)")
    regions.write.insertInto("regions")
    assert(snc.sql("select * from regions").count() == 4)

    val categories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/categories.csv").getPath)
    snc.sql("create table categories (" +
      "CategoryID int, " +
      "CategoryName string, " +
      "Description string, " +
      "Picture blob)")
    categories.write.insertInto("categories")
    assert(snc.sql("select * from categories").count() == 8)

    val shippers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/shippers.csv").getPath)
    snc.sql("create table shippers (" +
      "ShipperID int not null, " +
      "CompanyName string not null, " +
      "Phone string)")
    shippers.write.insertInto("shippers")
    assert(snc.sql("select * from shippers").count() == 3)

    val employees = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employees.csv").getPath)
    snc.sql("create table employees(" +
      "EmployeeID int not null , " +
      "LastName string not null, " +
      "FirstName string not null, " +
      "Title string, " +
      "TitleOfCourtesy string, " +
      "BirthDate timestamp, " +
      "HireDate timestamp, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "HomePhone string, " +
      "Extension string, " +
      "Photo blob, " +
      "Notes string, " +
      "ReportsTo int, " +
      "PhotoPath string)")
    employees.write.insertInto("employees")
    assert(snc.sql("select * from employees").count() == 8)

    val customers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/customers.csv").getPath)
    snc.sql("create table customers(" +
      "CustomerID string not null, " +
      "CompanyName string not null, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string)")
    customers.write.insertInto("customers")
    assert(snc.sql("select * from customers").count() == 91)

    val orders = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/orders.csv").getPath)
    snc.sql("create table orders (" +
      "OrderID int not null, " +
      "CustomerID string, " +
      "EmployeeID int, " +
      "OrderDate timestamp, " +
      "RequiredDate timestamp, " +
      "ShippedDate timestamp, " +
      "ShipVia int, " +
      "Freight double, " +
      "ShipName string, " +
      "ShipAddress string, " +
      "ShipCity string, " +
      "ShipRegion string, " +
      "ShipPostalCode string, " +
      "ShipCountry string) using row options (partition_by 'OrderId', buckets '13')")
    orders.write.insertInto("orders")
    assert(snc.sql("select * from orders").count() == 830)

    val order_details = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/order-details.csv").getPath)
    snc.sql("create table order_details (" +
      "OrderID int not null, " +
      "ProductID int not null, " +
      "UnitPrice double not null, " +
      "Quantity smallint not null, " +
      "Discount double not null) using row options (" +
      " partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders')")
    order_details.write.insertInto("order_details")
    assert(snc.sql("select * from order_details").count() == 2155)

    val products = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/products.csv").getPath)
    snc.sql("create table products(" +
      "ProductID int not null, " +
      "ProductName string, " +
      "SupplierID int not null, " +
      "CategoryID int not null," +
      "QuantityPerUnit string, " +
      "UnitPrice double, " +
      "UnitsInStock smallint, " +
      "UnitsOnOrder smallint," +
      "ReorderLevel smallint, " +
      "Discontinued smallint) USING row options ( partition_by 'ProductID', buckets '17')")
    products.write.insertInto("products")
    assert(snc.sql("select * from products").count() == 77)

    val suppliers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/suppliers.csv").getPath)
    snc.sql("create table suppliers(" +
      "SupplierID int not null, " +
      "CompanyName string not null, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string, " +
      "HomePage string) USING row options (PARTITION_BY 'SupplierID', buckets '123' )")
    suppliers.write.insertInto("suppliers")
    assert(snc.sql("select * from suppliers").count() == 29)

    val territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/territories.csv").getPath)
    snc.sql("create table territories(" +
      "TerritoryID string not null, " +
      "TerritoryDescription string not null, " +
      "RegionID string not null) using row options (partition_by 'TerritoryID', buckets '3')")
    territories.write.insertInto("territories")
    assert(snc.sql("select * from territories").count() == 53)

    val employee_territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employee-territories.csv").getPath)
    snc.sql("create table employee_territories(" +
      "EmployeeID int not null, " +
      "TerritoryID int not null) using row options(partition_by 'EmployeeID', buckets '1') ")
    employee_territories.write.insertInto("employee_territories")
    assert(snc.sql("select * from employee_territories").count() == 49)

  }
  private def createAndLoadColumnTables(snc: SnappyContext): Unit = {

    val regions = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/regions.csv").getPath)
    snc.sql("create table regions (" +
      "RegionID int, " +
      "RegionDescription string)")
    regions.write.insertInto("regions")
    assert(snc.sql("select * from regions").count() == 4)

    val categories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/categories.csv").getPath)
    snc.sql("create table categories (" +
      "CategoryID int, " +
      "CategoryName string, " +
      "Description string, " +
      "Picture blob)")
    categories.write.insertInto("categories")
    assert(snc.sql("select * from categories").count() == 8)

    val shippers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/shippers.csv").getPath)
    snc.sql("create table shippers (" +
      "ShipperID int not null, " +
      "CompanyName string not null, " +
      "Phone string)")
    shippers.write.insertInto("shippers")
    assert(snc.sql("select * from shippers").count() == 3)

    val employees = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employees.csv").getPath)
    snc.sql("create table employees(" +
      "EmployeeID int, " +
      "LastName string, " +
      "FirstName string, " +
      "Title string, " +
      "TitleOfCourtesy string, " +
      "BirthDate timestamp, " +
      "HireDate timestamp, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "HomePhone string, " +
      "Extension string, " +
      "Photo blob, " +
      "Notes string, " +
      "ReportsTo int, " +
      "PhotoPath string) using column options()")
    employees.write.insertInto("employees")
    assert(snc.sql("select * from employees").count() == 8)

    val customers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/customers.csv").getPath)
    snc.sql("create table customers(" +
      "CustomerID string, " +
      "CompanyName string, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string)")
    customers.write.insertInto("customers")
    assert(snc.sql("select * from customers").count() == 91)

    val orders = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/orders.csv").getPath)
    snc.sql("create table orders (" +
      "OrderID int, " +
      "CustomerID string, " +
      "EmployeeID int, " +
      "OrderDate timestamp, " +
      "RequiredDate timestamp, " +
      "ShippedDate timestamp, " +
      "ShipVia int, " +
      "Freight double, " +
      "ShipName string, " +
      "ShipAddress string, " +
      "ShipCity string, " +
      "ShipRegion string, " +
      "ShipPostalCode string, " +
      "ShipCountry string) using column options (partition_by 'OrderId', buckets '13')")
    orders.write.insertInto("orders")
    assert(snc.sql("select * from orders").count() == 830)

    val order_details = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/order-details.csv").getPath)
    snc.sql("create table order_details (" +
      "OrderID int, " +
      "ProductID int, " +
      "UnitPrice double, " +
      "Quantity int, " +
      "Discount double) using column options (" +
      " partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders')")
    order_details.write.insertInto("order_details")
    assert(snc.sql("select * from order_details").count() == 2155)

    val products = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/products.csv").getPath)
    snc.sql("create table products(" +
      "ProductID int, " +
      "ProductName string, " +
      "SupplierID int, " +
      "CategoryID int," +
      "QuantityPerUnit string, " +
      "UnitPrice double, " +
      "UnitsInStock int, " +
      "UnitsOnOrder int," +
      "ReorderLevel int, " +
      "Discontinued int) USING column options ( partition_by 'ProductID,SupplierID', buckets '17')")
    products.write.insertInto("products")
    assert(snc.sql("select * from products").count() == 77)

    val suppliers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/suppliers.csv").getPath)
    snc.sql("create table suppliers(" +
      "SupplierID int, " +
      "CompanyName string, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string, " +
      "HomePage string) USING column options (PARTITION_BY 'SupplierID', buckets '123' )")
    suppliers.write.insertInto("suppliers")
    assert(snc.sql("select * from suppliers").count() == 29)

    val territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/territories.csv").getPath)
    snc.sql("create table territories(" +
      "TerritoryID string , " +
      "TerritoryDescription string, " +
      "RegionID string) using column options (partition_by 'TerritoryID', buckets '3')")
    territories.write.insertInto("territories")
    assert(snc.sql("select * from territories").count() == 53)

    val employee_territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employee-territories.csv").getPath)
    snc.sql("create table employee_territories(" +
      "EmployeeID int, " +
      "TerritoryID int) using row options(partition_by 'EmployeeID', buckets '1') ")
    employee_territories.write.insertInto("employee_territories")
    assert(snc.sql("select * from employee_territories").count() == 49)

  }

  private def validateReplicatedTableQueries(snc: SnappyContext): Unit = {


    // Exploring the Tables
    assertQuery(snc, "SELECT * FROM Categories"
      , 8, 1, classOf[PartitionedPhysicalRDD])


    assertQuery(snc, "SELECT * FROM Customers"
      , 91, 1, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT * FROM Orders"
      , 830, 1, classOf[PartitionedPhysicalRDD])

    // SELECTing Specific Columns
    assertQuery(snc, "SELECT FirstName, LastName FROM Employees"
      , 8, 1, classOf[PartitionedPhysicalRDD])

    // Sorting By Multiple Columns
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY LastName", 8, 1, classOf[Sort])

    // Sorting By Column Position
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY 1,3", 8, 1, classOf[Sort])

    // Ascending and Descending Sorts
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " ORDER BY Title ASC, LastName DESC", 8, 1, classOf[Sort])

    // Checking for Equality
    assertQuery(snc, "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'", 5, 1, classOf[PartitionedPhysicalRDD])

    // Checking for Inequality
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE Title <> 'Sales Representative'", 3, 1, classOf[Project])

    // Checking for Greater or Less Than
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE LastName >= 'N'", 2, 1, classOf[PartitionedPhysicalRDD])

    // Checking for NULL
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees " +
      " WHERE Region IS NULL", 0, 1, classOf[Project])

    // WHERE and ORDER BY
    assertQuery(snc, "SELECT FirstName, LastName" +
      " FROM Employees" +
      " WHERE LastName >= 'N'" +
      " ORDER BY LastName DESC", 2, 1, classOf[Sort])

    //    // Using the WHERE clause to check for equality or inequality
    //    assertQuery(snc, "SELECT OrderDate, ShippedDate, CustomerID, Freight" +
    //      " FROM Orders " +
    //      " WHERE OrderDate = '19-May-1997'", 0, 4, classOf[LocalTableScan])

    // Using WHERE and ORDER BY Together
    assertQuery(snc, "SELECT CompanyName, ContactName, Fax" +
      " FROM Customers" +
      " WHERE Fax IS NOT NULL" +
      " ORDER BY CompanyName", 91, 1, classOf[Sort])

    // The IN Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy IN ('Ms.','Mrs.')", 5, 1, classOf[PartitionedPhysicalRDD])

    // The LIKE Operator
    assertQuery(snc, "SELECT TitleOfCourtesy, FirstName, LastName" +
      " FROM Employees" +
      " WHERE TitleOfCourtesy LIKE 'M%'", 7, 1, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT FirstName, LastName, BirthDate" +
      " FROM Employees" +
      " WHERE BirthDate BETWEEN '1950-01-01' AND '1959-12-31 23:59:59'",
      3, 1, classOf[PartitionedPhysicalRDD])

    assertQuery(snc, "SELECT CONCAT(FirstName, ' ', LastName)" +
      " FROM Employees", 8, 1, classOf[Project])

    assertQuery(snc, "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal" +
      " FROM Orders" +
      " WHERE Freight >= 500", 13, 1, classOf[Project])

    assertQuery(snc, "SELECT SUM(Quantity) AS TotalUnits" +
      " FROM Order_Details" +
      " WHERE ProductID=3", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT MIN(HireDate) AS FirstHireDate," +
      " MAX(HireDate) AS LastHireDate" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT City, COUNT(EmployeeID) AS NumEmployees" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'" +
      " GROUP BY City" +
      " HAVING COUNT(EmployeeID) > 1" +
      " ORDER BY NumEmployees", 1, 1, classOf[Sort])

    assertQuery(snc, "SELECT COUNT(DISTINCT City) AS NumCities" +
      " FROM Employees", 1, 1, classOf[TungstenAggregate])

    assertQuery(snc, "SELECT ProductID, AVG(UnitPrice) AS AveragePrice" +
      " FROM Products " +
      " GROUP BY ProductID " +
      " HAVING AVG(UnitPrice) > 70" +
      " ORDER BY AveragePrice", 4, 1, classOf[Sort])

//    assertJoin(snc, "SELECT CompanyName FROM Customers WHERE CustomerID = " +
//      "(SELECT CustomerID FROM Orders WHERE OrderID = 10290)"
//      , 1, 1, classOf[LeftSemiJoinHash])

//    assertJoin(snc, "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
//      "FROM Orders WHERE OrderDate BETWEEN '1-Jan-1997' AND '31-Dec-1997')"
//      , 89, 1, classOf[LeftSemiJoinHash])

//    assertJoin(snc, "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
//      " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
//      "('Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders'))"
//      , 9, 1, classOf[LeftSemiJoinHash])

//    assertJoin(snc, "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
//      "CategoryID FROM Categories WHERE CategoryName = 'Seafood')"
//      , 12, 1, classOf[LeftSemiJoinHash])

//    assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
//      "(SELECT SupplierID FROM Products WHERE CategoryID = 8)"
//      , 8, 1, classOf[LeftSemiJoinHash])
//
//    assertJoin(snc, "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
//      " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
//      " WHERE CategoryName = 'Seafood'))", 8, 1, classOf[LeftSemiJoinHash])

    assertJoin(snc, "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " ORDER BY Orders.OrderDate", 758, 1, classOf[LocalJoin])

    //    assertJoin(snc, "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName" +
    //      " FROM Orders o" +
    //      " JOIN Employees e ON (e.EmployeeID = o.EmployeeID)" +
    //      " JOIN Customers c ON (c.CustomerID = o.CustomerID)" +
    //      " WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1-Jan-1998'" +
    //      " ORDER BY c.CompanyName" , 51, 1, classOf[LocalJoin])
    //
    //    assertJoin(snc, "SELECT e.FirstName, e.LastName, o.OrderID" +
    //      " FROM Employees e JOIN Orders o ON" +
    //      " (e.EmployeeID = o.EmployeeID)" +
    //      " WHERE o.RequiredDate < o.ShippedDate" +
    //      " ORDER BY e.LastName, e.FirstName" , 51, 1 , classOf[LocalJoin])

    assertJoin(snc, "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
      " FROM Order_Details od JOIN Products p ON" +
      " (p.ProductID = od.ProductID)" +
      " GROUP BY p.ProductName" +
      " HAVING SUM(Quantity) < 200", 5, 1, classOf[LocalJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e JOIN Customers c ON" +
      " (e.City = c.City)" +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC", 3, 4, classOf[LocalJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e LEFT JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC", 5, 4, classOf[SortMergeOuterJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City " +
      " FROM Employees e RIGHT JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City" +
      " ORDER BY numEmployees DESC", 69, 5, classOf[SortMergeOuterJoin])

    assertJoin(snc, "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City, c.City" +
      " FROM Employees e FULL JOIN Customers c ON" +
      " (e.City = c.City) " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC", 71, 5, classOf[SortMergeOuterJoin])

    assertJoin(snc, "select s.supplierid,s.companyname,p.productid,p.productname " +
      "from suppliers s join products p on(s.supplierid= p.supplierid) and" +
      " s.companyname IN('Grandma Kelly''s Homestead','Tokyo Traders','Exotic Liquids')"
      , 9, 1, classOf[LocalJoin])

    assertJoin(snc, "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID"
      , 830, 1, classOf[LocalJoin])

    assertJoin(snc, "SELECT order_details.OrderID,ShipCountry,UnitPrice,Quantity,Discount" +
      " FROM orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 1, classOf[LocalJoin])

    assertJoin(snc, "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry"
      , 22, 1, classOf[LocalJoin])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId"
      , 830, 1, classOf[LeftSemiJoinHash])

    assertJoin(snc, "SELECT * FROM orders LEFT SEMI JOIN order_details"
      , 830, 1, classOf[LeftSemiJoinBNL])

    assertJoin(snc, "SELECT * FROM orders JOIN order_details"
      , 1788650, 1, classOf[CartesianProduct])
    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details"
      , 1788650, 1, classOf[CartesianProduct])
    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details"
      , 1788650, 1, classOf[CartesianProduct])
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details"
      , 1788650, 1, classOf[CartesianProduct])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details"
      , 1788650, 1, classOf[CartesianProduct])

    assertJoin(snc, "SELECT * FROM orders JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 1, classOf[LocalJoin])
    assertJoin(snc, "SELECT * FROM orders LEFT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 1, classOf[SortMergeOuterJoin])
    assertJoin(snc, "SELECT * FROM orders RIGHT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 1, classOf[SortMergeOuterJoin])
    assertJoin(snc, "SELECT * FROM orders FULL OUTER JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 1, classOf[SortMergeOuterJoin])
    assertJoin(snc, "SELECT * FROM orders FULL JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
      , 2155, 1, classOf[SortMergeOuterJoin])
  }



  private def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {

    val regions = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/regions.csv").getPath)
    snc.sql("create table regions (" +
      "RegionID int, " +
      "RegionDescription string)")
    regions.write.insertInto("regions")
    assert(snc.sql("select * from regions").count() == 4)

    val categories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/categories.csv").getPath)
    snc.sql("create table categories (" +
      "CategoryID int, " +
      "CategoryName string, " +
      "Description string, " +
      "Picture blob)")
    categories.write.insertInto("categories")
    assert(snc.sql("select * from categories").count() == 8)

    val shippers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/shippers.csv").getPath)
    snc.sql("create table shippers (" +
      "ShipperID int not null, " +
      "CompanyName string not null, " +
      "Phone string)")
    shippers.write.insertInto("shippers")
    assert(snc.sql("select * from shippers").count() == 3)

    val employees = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employees.csv").getPath)
    snc.sql("create table employees(" +
      "EmployeeID int not null , " +
      "LastName string not null, " +
      "FirstName string not null, " +
      "Title string, " +
      "TitleOfCourtesy string, " +
      "BirthDate timestamp, " +
      "HireDate timestamp, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "HomePhone string, " +
      "Extension string, " +
      "Photo blob, " +
      "Notes string, " +
      "ReportsTo int, " +
      "PhotoPath string)")
    employees.write.insertInto("employees")
    assert(snc.sql("select * from employees").count() == 8)

    val customers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/customers.csv").getPath)
    snc.sql("create table customers(" +
      "CustomerID string not null, " +
      "CompanyName string not null, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string)")
    customers.write.insertInto("customers")
    assert(snc.sql("select * from customers").count() == 91)

    val orders = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/orders.csv").getPath)
    snc.sql("create table orders (" +
      "OrderID int not null, " +
      "CustomerID string, " +
      "EmployeeID int, " +
      "OrderDate timestamp, " +
      "RequiredDate timestamp, " +
      "ShippedDate timestamp, " +
      "ShipVia int, " +
      "Freight double, " +
      "ShipName string, " +
      "ShipAddress string, " +
      "ShipCity string, " +
      "ShipRegion string, " +
      "ShipPostalCode string, " +
      "ShipCountry string)")
    orders.write.insertInto("orders")
    assert(snc.sql("select * from orders").count() == 830)

    val order_details = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/order-details.csv").getPath)
    snc.sql("create table order_details (" +
      "OrderID int not null, " +
      "ProductID int not null, " +
      "UnitPrice double not null, " +
      "Quantity smallint not null, " +
      "Discount double not null)")
    order_details.write.insertInto("order_details")
    assert(snc.sql("select * from order_details").count() == 2155)

    val products = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/products.csv").getPath)
    snc.sql("create table products(" +
      "ProductID int not null, " +
      "ProductName string, " +
      "SupplierID int not null, " +
      "CategoryID int not null," +
      "QuantityPerUnit string, " +
      "UnitPrice double, " +
      "UnitsInStock smallint, " +
      "UnitsOnOrder smallint," +
      "ReorderLevel smallint, " +
      "Discontinued smallint) ")
    // "USING row options ( partition_by 'ProductID', buckets '17')")
    products.write.insertInto("products")
    assert(snc.sql("select * from products").count() == 77)

    val suppliers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/suppliers.csv").getPath)
    snc.sql("create table suppliers(" +
      "SupplierID int not null, " +
      "CompanyName string not null, " +
      "ContactName string, " +
      "ContactTitle string, " +
      "Address string, " +
      "City string, " +
      "Region string, " +
      "PostalCode string, " +
      "Country string, " +
      "Phone string, " +
      "Fax string, " +
      "HomePage string) ")
    // "USING row options (PARTITION_BY 'SupplierID', buckets '123' )")
    suppliers.write.insertInto("suppliers")
    assert(snc.sql("select * from suppliers").count() == 29)

    val territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/territories.csv").getPath)
    snc.sql("create table territories(" +
      "TerritoryID string not null, " +
      "TerritoryDescription string not null, " +
      "RegionID string not null)")
    territories.write.insertInto("territories")
    assert(snc.sql("select * from territories").count() == 53)

    val employee_territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(getClass.getResource("/northwind/employee-territories.csv").getPath)
    snc.sql("create table employee_territories(" +
      "EmployeeID int not null, " +
      "TerritoryID int not null)")
    employee_territories.write.insertInto("employee_territories")
    assert(snc.sql("select * from employee_territories").count() == 49)
  }

  private def dropTables(snc: SnappyContext): Unit = {
    snc.sql("drop table if exists regions")
    snc.sql("drop table if exists categories")
    snc.sql("drop table if exists products")
    snc.sql("drop table if exists order_details")
    snc.sql("drop table if exists orders")
    snc.sql("drop table if exists customers")
    snc.sql("drop table if exists employees")
    snc.sql("drop table if exists employee_territories")
    snc.sql("drop table if exists shippers")
    snc.sql("drop table if exists suppliers")
    snc.sql("drop table if exists territories")
  }

}
