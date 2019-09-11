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

object HiveMetaStoreUtils {
  val setExternalHiveCatalog: String =
    "set spark.sql.catalogImplementation=hive"
  val setExternalInBuiltCatalog: String =
    "set spark.sql.catalogImplementation=in-memory"
  val showTblsApp: String =
    "show tables in app"
  val showTblsDefault: String =
    "show tables in default"
  val dropTable: String =
    "drop table if exists "
  val createDB: String =
    "create database "

  val beeLineQueries = new Array[String](4)
  val snappyQueries = new Array[String] (4)
  beeLineQueries(0) = "SELECT CategoryID,CategoryName,Description FROM hive_db.hive_categories" +
    " where CategoryID is not null"
  beeLineQueries(1) = "SELECT FirstName, LastName FROM hive_db.hive_employees " +
    "where FirstName <> 'FIRSTNAME'  ORDER BY LastName "
  beeLineQueries(2) = "SELECT FirstName, LastName FROM hive_db.hive_employees" +
    " WHERE Title = 'Sales Representative'"
  beeLineQueries(3) = "SELECT SUM(Quantity) AS TotalUnits FROM hive_db.hive_order_details" +
    " WHERE ProductID=3"

  snappyQueries(0) = "SELECT CategoryID,CategoryName,Description FROM tibco_db.snappy_categories"
  snappyQueries(1) = "SELECT FirstName, LastName FROM tibco_db.snappy_employees ORDER BY LastName"
  snappyQueries(2) = "SELECT FirstName, LastName FROM tibco_db.snappy_employees" +
    " WHERE Title = 'Sales Representative'"
  snappyQueries(3) = "SELECT SUM(Quantity) AS TotalUnits FROM tibco_db.snappy_order_details" +
    " WHERE ProductID=3"

  // SELECTing Specific Columns
  val Q4: String = "SELECT FirstName, LastName FROM Employees"

  // Sorting By Column Position
  val Q6: String = "SELECT Title, FirstName, LastName" +
    " FROM Employees" +
    " ORDER BY 1,3"

  // Ascending and Descending Sorts
  val Q7: String = "SELECT Title, FirstName, LastName" +
    " FROM Employees " +
    " ORDER BY Title ASC, LastName DESC"


  // Checking for Inequality
  val Q9: String = "SELECT FirstName, LastName" +
    " FROM Employees" +
    " WHERE Title <> 'Sales Representative'"

  // Checking for Greater or Less Than
  val Q10: String = "SELECT FirstName, LastName" +
    " FROM Employees " +
    " WHERE LastName >= 'N'"

  // Checking for NULL
  val Q11: String = "SELECT FirstName, LastName" +
    " FROM Employees " +
    " WHERE Region IS NULL"

  // WHERE and ORDER BY
  val Q12: String = "SELECT FirstName, LastName" +
    " FROM Employees" +
    " WHERE LastName >= 'N'" +
    " ORDER BY LastName DESC"

  // Using the WHERE clause to check for equality or inequality
  val Q13: String = "SELECT OrderDate, ShippedDate, CustomerID, Freight" +
    " FROM Orders " +
    " WHERE OrderDate = Cast('1997-05-19' as TIMESTAMP)"

  // Using WHERE and ORDER BY Together
  val Q14: String = "SELECT CompanyName, ContactName, Fax" +
    " FROM Customers" +
    " WHERE Fax IS NOT NULL" +
    " ORDER BY CompanyName"

  // The IN Operator
  val Q15: String = "SELECT TitleOfCourtesy, FirstName, LastName" +
    " FROM Employees" +
    " WHERE TitleOfCourtesy IN ('Ms.','Mrs.')"

  // The LIKE Operator
  val Q16: String = "SELECT TitleOfCourtesy, FirstName, LastName" +
    " FROM Employees" +
    " WHERE TitleOfCourtesy LIKE 'M%'"

  val Q17: String = "SELECT FirstName, LastName, BirthDate" +
    " FROM Employees" +
    " WHERE BirthDate BETWEEN Cast('1950-01-01' as TIMESTAMP) AND " +
    "Cast('1959-12-31 23:59:59' as TIMESTAMP)"

  val Q18: String = "SELECT CONCAT(FirstName, ' ', LastName)" +
    " FROM Employees"

  val Q19: String = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal" +
    " FROM Orders" +
    " WHERE Freight >= 500"

  val Q20: String = "SELECT SUM(Quantity) AS TotalUnits" +
    " FROM Order_Details" +
    " WHERE ProductID=3"

  val Q21: String = "SELECT MIN(HireDate) AS FirstHireDate," +
    " MAX(HireDate) AS LastHireDate" +
    " FROM Employees"

  val Q22: String = "SELECT City, COUNT(EmployeeID) AS NumEmployees" +
    " FROM Employees " +
    " WHERE Title = 'Sales Representative'" +
    " GROUP BY City" +
    " HAVING COUNT(EmployeeID) > 1" +
    " ORDER BY NumEmployees"

  val Q23: String = "SELECT COUNT(DISTINCT City) AS NumCities" +
    " FROM Employees"

  val Q24: String = "SELECT ProductID, AVG(UnitPrice) AS AveragePrice" +
    " FROM Products " +
    " GROUP BY ProductID " +
    " HAVING AVG(UnitPrice) > 70" +
    " ORDER BY AveragePrice"

  val Q25: String = "SELECT CompanyName FROM Customers WHERE CustomerID = " +
    "(SELECT CustomerID FROM Orders WHERE OrderID = 10290)"

  val Q25_1: String = "SELECT CompanyName FROM Customers WHERE CustomerID = " +
    "(SELECT CustomerID FROM Orders WHERE OrderID = 10295)"

  val Q25_2: String = "SELECT CompanyName FROM Customers WHERE CustomerID = " +
    "(SELECT CustomerID FROM Orders WHERE OrderID = 10391)"

  val Q26: String = "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
    "FROM Orders WHERE OrderDate BETWEEN Cast('1997-01-01' as TIMESTAMP) AND " +
    "Cast('1997-12-31' as TIMESTAMP))"

  val Q26_1: String = "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
    "FROM Orders WHERE OrderDate BETWEEN Cast('1997-09-30' as TIMESTAMP) AND " +
    "Cast('1997-12-24' as TIMESTAMP))"

  val Q26_2: String = "SELECT CompanyName FROM Customers  WHERE CustomerID IN (SELECT CustomerID " +
    "FROM Orders WHERE OrderDate BETWEEN Cast('1997-10-01' as TIMESTAMP) AND " +
    "Cast('1997-12-31' as TIMESTAMP))"

  val Q27: String = "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    "('Exotic Liquids', 'Grandma Kellys Homestead', 'Tokyo Traders'))"

  val Q27_1: String = "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    "('Pavlova Ltd.'))"

  val Q27_2: String = "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    "('Pavlova Ltd.', 'Karkki Oy'))"

  val Q27_3: String = "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    "('Grandma Kellys Homestead'))"

  val Q27_4: String = "SELECT ProductName, SupplierID FROM Products WHERE SupplierID" +
    " IN (SELECT SupplierID FROM Suppliers WHERE CompanyName IN" +
    "('Exotic Liquids', 'Karkki Oy'))"

  val Q28: String = "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
    "CategoryID FROM Categories WHERE CategoryName = 'Seafood')"

  val Q28_1: String = "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
    "CategoryID FROM Categories WHERE CategoryName = 'Condiments')"

  val Q28_2: String = "SELECT ProductName FROM Products WHERE CategoryID = (SELECT " +
    "CategoryID FROM Categories WHERE CategoryName = 'Produce')"

  val Q29: String = "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
    "(SELECT SupplierID FROM Products WHERE CategoryID = 8)"

  val Q29_1: String = "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
    "(SELECT SupplierID FROM Products WHERE CategoryID = 5)"

  val Q29_2: String = "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN " +
    "(SELECT SupplierID FROM Products WHERE CategoryID = 3)"

  val Q30: String = "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
    " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
    " WHERE CategoryName = 'Seafood'))"

  val Q30_1: String = "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
    " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
    " WHERE CategoryName = 'Condiments'))"

  val Q30_2: String = "SELECT CompanyName  FROM Suppliers WHERE SupplierID IN (SELECT SupplierID" +
    " FROM Products  WHERE CategoryID = (SELECT CategoryID FROM Categories" +
    " WHERE CategoryName = 'Confections'))"









  val joinHiveSnappy = new Array[String](7)
  val validateJoin = new Array[String](7)
  joinHiveSnappy(0) = "SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID," +
    " o.OrderDate FROM default.hive_employees emp JOIN app.snappy_orders o ON " +
    "(emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderID"
  joinHiveSnappy(1) = "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName " +
    "FROM default.hive_orders o JOIN default.hive_employees e ON (e.EmployeeID = o.EmployeeID) " +
    "JOIN snappy_customers c ON (c.CustomerID = o.CustomerID) " +
    "WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) " +
    "ORDER BY o.OrderID"
  joinHiveSnappy(2) = "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
    " FROM default.hive_order_details od JOIN snappy_products p ON" +
    " (p.ProductID = od.ProductID)" +
    " GROUP BY p.ProductName" +
    " HAVING SUM(Quantity) >10 and SUM(Quantity) <100"
  joinHiveSnappy(3) = "SELECT emp.EmployeeID,emp.FirstName,emp.LastName," +
    "o.OrderID,o.OrderDate FROM" +
    " snappy_employees emp JOIN default.hive_orders o ON " +
    "(emp.EmployeeID = o.EmployeeID) " +
    "where o.EmployeeID < 5 ORDER BY o.OrderID"
  joinHiveSnappy(4) = "SELECT o.OrderID,c.CompanyName,e.FirstName, e.LastName" +
    " FROM default.hive_orders o JOIN default.hive_employees e " +
    "ON (e.EmployeeID = o.EmployeeID) JOIN snappy_customers c " +
    "ON (c.CustomerID = o.CustomerID) " +
    "WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) " +
    "ORDER BY c.CompanyName"
  joinHiveSnappy(5) = "SELECT e.FirstName, e.LastName, o.OrderID FROM " +
    "snappy_employees e JOIN default.hive_orders o ON " +
    "(e.EmployeeID = o.EmployeeID) " +
    "WHERE o.RequiredDate < o.ShippedDate " +
    "ORDER BY e.LastName, e.FirstName"
  joinHiveSnappy(6) = "select distinct (a.ShippedDate) as ShippedDate,a.OrderID," +
    "b.Subtotal,year(a.ShippedDate) as Year " +
    "from snappy_orders a inner join" +
    "(select distinct OrderID,sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal " +
    "from default.hive_order_details group by OrderID) b " +
    "on a.OrderID = b.OrderID " +
    "where a.ShippedDate is not null and " +
    "a.ShippedDate > Cast('1996-12-24' as TIMESTAMP) and " +
    "a.ShippedDate < Cast('1997-09-30' as TIMESTAMP) " +
    "order by ShippedDate"

    validateJoin(0) = "SELECT emp.EmployeeID, emp.FirstName, emp.LastName, o.OrderID," +
    " o.OrderDate FROM snappy_employees emp JOIN snappy_orders o ON " +
    "(emp.EmployeeID = o.EmployeeID) ORDER BY o.OrderID"
    validateJoin(1) = "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName " +
      "FROM snappy_orders o JOIN snappy_employees e ON (e.EmployeeID = o.EmployeeID) " +
      "JOIN snappy_customers c ON (c.CustomerID = o.CustomerID) " +
      "WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) " +
      "ORDER BY o.OrderID"
  validateJoin(2) = "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
    " FROM snappy_order_details od JOIN snappy_products p ON" +
    " (p.ProductID = od.ProductID)" +
    " GROUP BY p.ProductName" +
    " HAVING SUM(Quantity) >10 and SUM(Quantity) <100"
  validateJoin(3) = "SELECT emp.EmployeeID,emp.FirstName,emp.LastName," +
    "o.OrderID,o.OrderDate" +
    " FROM snappy_employees emp JOIN snappy_orders o ON " +
    "(emp.EmployeeID = o.EmployeeID) " +
    "where o.EmployeeID < 5 ORDER BY o.OrderID"
  validateJoin(4) = "SELECT o.OrderID,c.CompanyName,e.FirstName, e.LastName" +
    " FROM snappy_orders o JOIN snappy_employees e " +
    "ON (e.EmployeeID = o.EmployeeID) JOIN snappy_customers c " +
    "ON (c.CustomerID = o.CustomerID) " +
    "WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP) " +
    "ORDER BY c.CompanyName"
  validateJoin(5) = "SELECT e.FirstName, e.LastName, o.OrderID FROM " +
    "snappy_employees e JOIN snappy_orders o ON " +
    "(e.EmployeeID = o.EmployeeID) " +
    "WHERE o.RequiredDate < o.ShippedDate " +
    "ORDER BY e.LastName, e.FirstName"
  validateJoin(6) = "select distinct (a.ShippedDate) as ShippedDate,a.OrderID," +
    "b.Subtotal,year(a.ShippedDate) as Year " +
    "from snappy_orders a inner join" +
    "(select distinct OrderID,sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal " +
    "from snappy_order_details group by OrderID) b " +
    "on a.OrderID = b.OrderID " +
    "where a.ShippedDate is not null and " +
    "a.ShippedDate > Cast('1996-12-24' as TIMESTAMP) and " +
    "a.ShippedDate < Cast('1997-09-30' as TIMESTAMP) " +
    "order by ShippedDate"
}
