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

  val beeLineQueries = new Array[String](11)
  val snappyQueries = new Array[String] (11)
  beeLineQueries(0) = "SELECT CategoryID,CategoryName,Description FROM hive_db.hive_categories" +
    " where CategoryID is not null"
  beeLineQueries(1) = "SELECT FirstName, LastName FROM hive_db.hive_employees " +
    "where FirstName <> 'FIRSTNAME'  ORDER BY LastName "
  beeLineQueries(2) = "SELECT FirstName, LastName FROM hive_db.hive_employees" +
    " WHERE Title = 'Sales Representative'"
  beeLineQueries(3) = "SELECT SUM(Quantity) AS TotalUnits FROM hive_db.hive_order_details" +
    " WHERE ProductID=3"
  beeLineQueries(4) = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal " +
    "FROM hive_db.hive_orders WHERE Freight >= 500"
  beeLineQueries(5) = "SELECT City, COUNT(EmployeeID) AS NumEmployees " +
    "FROM hive_db.hive_employees " +
    "WHERE Title = 'Sales Representative' " +
    "GROUP BY City HAVING COUNT(EmployeeID) > 1 ORDER BY NumEmployees"
  beeLineQueries(6) = "SELECT ProductID, AVG(UnitPrice) AS AveragePrice " +
    "FROM hive_db.hive_products GROUP BY ProductID " +
    "HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice"
  beeLineQueries(7) = "SELECT CompanyName FROM hive_db.hive_customers WHERE CustomerID = " +
    "(SELECT CustomerID FROM hive_db.hive_orders WHERE OrderID = 687792)"
  beeLineQueries(8) = "SELECT ProductName, SupplierID FROM hive_db.hive_products " +
    "WHERE SupplierID IN " +
    "(SELECT SupplierID FROM hive_db.hive_suppliers " +
    "WHERE CompanyName IN ('Exotic Liquids', 'Grandma Kellys Homestead', 'Tokyo Traders')) " +
    "order by SupplierID,ProductName"
  beeLineQueries(9) = "SELECT CompanyName FROM hive_db.hive_suppliers " +
    "WHERE SupplierID IN " +
    "(SELECT SupplierID FROM hive_db.hive_products  " +
    "WHERE CategoryID = " +
    "(SELECT CategoryID FROM hive_db.hive_categories " +
    "WHERE CategoryName = 'Seafood')) order by CompanyName"
  beeLineQueries(10) = "SELECT ProductID,Productname,unitprice,unitsinstock " +
    "from hive_db.hive_products where productname like 'M%' order by productname"

  snappyQueries(0) = "SELECT CategoryID,CategoryName,Description FROM tibco_db.snappy_categories"
  snappyQueries(1) = "SELECT FirstName, LastName FROM tibco_db.snappy_employees ORDER BY LastName"
  snappyQueries(2) = "SELECT FirstName, LastName FROM tibco_db.snappy_employees" +
    " WHERE Title = 'Sales Representative'"
  snappyQueries(3) = "SELECT SUM(Quantity) AS TotalUnits FROM tibco_db.snappy_order_details" +
    " WHERE ProductID=3"
  snappyQueries(4) = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal " +
    "FROM tibco_db.snappy_orders WHERE Freight >= 500"
  snappyQueries(5) = "SELECT City, COUNT(EmployeeID) AS NumEmployees " +
    "FROM tibco_db.snappy_employees " +
    "WHERE Title = 'Sales Representative' " +
    "GROUP BY City HAVING COUNT(EmployeeID) > 1 ORDER BY NumEmployees;"
  snappyQueries(6) = "SELECT ProductID, AVG(UnitPrice) AS AveragePrice " +
    "FROM tibco_db.snappy_products GROUP BY ProductID " +
    "HAVING AVG(UnitPrice) > 70 ORDER BY AveragePrice"
  snappyQueries(7) = "SELECT CompanyName FROM tibco_db.snappy_customers WHERE CustomerID = " +
    "(SELECT CustomerID FROM tibco_db.snappy_orders WHERE OrderID = 687792)"
  snappyQueries(8) = "SELECT ProductName, SupplierID FROM tibco_db.snappy_products " +
    "WHERE SupplierID IN " +
    "(SELECT SupplierID FROM tibco_db.snappy_suppliers " +
    "WHERE CompanyName IN ('Exotic Liquids', 'Grandma Kellys Homestead', 'Tokyo Traders')) " +
    "order by SupplierID,ProductName"
  snappyQueries(9) = "SELECT CompanyName FROM tibco_db.snappy_suppliers " +
    "WHERE SupplierID IN " +
    "(SELECT SupplierID FROM tibco_db.snappy_products  " +
    "WHERE CategoryID = " +
    "(SELECT CategoryID FROM tibco_db.snappy_categories " +
    "WHERE CategoryName = 'Seafood')) order by CompanyName"
  snappyQueries(10) = "SELECT ProductID,Productname,unitprice,unitsinstock " +
    "from hive_db.hive_products where productname like 'M%' order by productname"

  val joinHiveSnappy = new Array[String](10)
  val validateJoin = new Array[String](10)
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
  joinHiveSnappy(7) = "select distinct a.CategoryID,a.CategoryName,b.ProductName," +
    "sum(c.ExtendedPrice) as ProductSales from app.snappy_categories a " +
    "inner join default.hive_products b on a.CategoryID = b.CategoryID " +
    "inner join " +
    "(select distinct y.OrderID,y.ProductID,x.ProductName,y.UnitPrice,y.Quantity,y.Discount," +
    "round(y.UnitPrice * y.Quantity * (1 - y.Discount), 2) as ExtendedPrice from " +
    "default.hive_products x " +
    "inner join app.snappy_order_details y on x.ProductID = y.ProductID " +
    "order by y.OrderID) c on c.ProductID = b.ProductID " +
    "inner join default.hive_orders d on d.OrderID =c.OrderID " +
    "where d.OrderDate > Cast('1997-01-01' as TIMESTAMP) and " +
    "d.OrderDate < Cast('1997-12-31' as TIMESTAMP) " +
    "group by a.CategoryID, a.CategoryName, b.ProductName " +
    "order by a.CategoryName, b.ProductName, ProductSales"
  joinHiveSnappy(8) = "select s.supplierid,s.companyname,p.productid,p.productname from " +
    "app.snappy_suppliers s join app.snappy_products p on(s.supplierid= p.supplierid) " +
    "and s.companyname IN('Grandma Kellys Homestead','Tokyo Traders','Exotic Liquids') " +
    "order by companyname,productname"
  joinHiveSnappy(9) = "SELECT ShipCountry,Sum(hive_order_details.UnitPrice * Quantity * Discount)" +
    " AS ProductSales FROM app.snappy_orders INNER JOIN default.hive_order_details " +
    "ON snappy_orders.OrderID = hive_order_details.OrderID " +
    "where snappy_orders.OrderID > 11000 GROUP BY ShipCountry order by ProductSales"

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
  validateJoin(7) = "select distinct a.CategoryID,a.CategoryName,b.ProductName," +
    "sum(c.ExtendedPrice) as ProductSales from app.snappy_categories a " +
    "inner join app.snappy_products b on a.CategoryID = b.CategoryID " +
    "inner join " +
    "(select distinct y.OrderID,y.ProductID,x.ProductName,y.UnitPrice,y.Quantity,y.Discount," +
    "round(y.UnitPrice * y.Quantity * (1 - y.Discount), 2) as ExtendedPrice " +
    "from app.snappy_products x inner join app.snappy_order_details y " +
    "on x.ProductID = y.ProductID order by y.OrderID) c on c.ProductID = b.ProductID " +
    "inner join app.snappy_orders d on d.OrderID =c.OrderID " +
    "where d.OrderDate > Cast('1997-01-01' as TIMESTAMP) and " +
    "d.OrderDate < Cast('1997-12-31' as TIMESTAMP) " +
    "group by a.CategoryID, a.CategoryName, b.ProductName " +
    "order by a.CategoryName, b.ProductName, ProductSales"
  validateJoin(8) = "select s.supplierid,s.companyname,p.productid,p.productname from " +
    "app.snappy_suppliers s join app.snappy_products p on(s.supplierid= p.supplierid) " +
    "and s.companyname IN('Grandma Kellys Homestead','Tokyo Traders','Exotic Liquids') " +
    "order by companyname,productname"
  validateJoin(9) = "SELECT ShipCountry,Sum(snappy_order_details.UnitPrice * Quantity * Discount)" +
    " AS ProductSales FROM app.snappy_orders INNER JOIN app.snappy_order_details " +
    "ON snappy_orders.OrderID = snappy_order_details.OrderID " +
    "where snappy_orders.OrderID > 11000 GROUP BY ShipCountry order by ProductSales"
}
