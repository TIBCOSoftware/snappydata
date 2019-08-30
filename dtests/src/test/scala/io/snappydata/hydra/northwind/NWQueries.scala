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
package io.snappydata.hydra.northwind

import org.apache.spark.sql.{SQLContext, DataFrame, SnappyContext}

object NWQueries {
  var snc: SnappyContext = _
  var dataFilesLocation: String = _
  val Q1: String = "SELECT CategoryID,CategoryName,Description FROM Categories"
  val Q2: String = "SELECT * FROM Customers"
  val Q3: String = "SELECT * FROM Orders"
  // SELECTing Specific Columns
  val Q4: String = "SELECT FirstName, LastName FROM Employees"
  // Sorting By Multiple Columns
  val Q5: String = "SELECT FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY LastName"

  // Sorting By Column Position
  val Q6: String = "SELECT Title, FirstName, LastName" +
      " FROM Employees" +
      " ORDER BY 1,3"

  // Ascending and Descending Sorts
  val Q7: String = "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " ORDER BY Title ASC, LastName DESC"

  // Checking for Equality
  val Q8: String = "SELECT Title, FirstName, LastName" +
      " FROM Employees " +
      " WHERE Title = 'Sales Representative'"

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

  val Q31: String = "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " ORDER BY Orders.OrderDate"

  val Q31_1: String = "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " where Orders.EmployeeID < 5" +
      " ORDER BY Orders.OrderDate"

  val Q31_2: String = "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " where Orders.EmployeeID > 5" +
      " ORDER BY Orders.OrderDate"

  val Q31_3: String = "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " where Orders.EmployeeID < 3" +
      " ORDER BY Orders.OrderDate"

  val Q31_4: String = "SELECT Employees.EmployeeID, Employees.FirstName," +
      " Employees.LastName, Orders.OrderID, Orders.OrderDate" +
      " FROM Employees JOIN Orders ON" +
      " (Employees.EmployeeID = Orders.EmployeeID)" +
      " where Orders.EmployeeID > 3" +
      " ORDER BY Orders.OrderDate"

  val Q32: String = "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName" +
      " FROM Orders o" +
      " JOIN Employees e ON (e.EmployeeID = o.EmployeeID)" +
      " JOIN Customers c ON (c.CustomerID = o.CustomerID)" +
      " WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > Cast('1998-01-01' as TIMESTAMP)" +
      " ORDER BY c.CompanyName"

  val Q32_1: String = "SELECT o.OrderID, c.CompanyName, e.FirstName, e.LastName" +
      " FROM Orders o" +
      " JOIN Employees e ON (e.EmployeeID = o.EmployeeID)" +
      " JOIN Customers c ON (c.CustomerID = o.CustomerID)" +
      " WHERE o.ShippedDate < o.RequiredDate AND o.OrderDate > Cast('1997-12-01' as TIMESTAMP)" +
      " ORDER BY c.CompanyName"

  val Q33: String = "SELECT e.FirstName, e.LastName, o.OrderID" +
      " FROM Employees e JOIN Orders o ON" +
      " (e.EmployeeID = o.EmployeeID)" +
      " WHERE o.RequiredDate < o.ShippedDate" +
      " ORDER BY e.LastName, e.FirstName"

  val Q33_1: String = "SELECT e.FirstName, e.LastName, o.OrderID" +
      " FROM Employees e JOIN Orders o ON" +
      " (e.EmployeeID = o.EmployeeID)" +
      " WHERE o.RequiredDate > o.ShippedDate" +
      " ORDER BY e.LastName, e.FirstName"

  val Q34: String = "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
      " FROM Order_Details od JOIN Products p ON" +
      " (p.ProductID = od.ProductID)" +
      " GROUP BY p.ProductName" +
      " HAVING SUM(Quantity) < 200"

  val Q34_1: String = "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
      " FROM Order_Details od JOIN Products p ON" +
      " (p.ProductID = od.ProductID)" +
      " GROUP BY p.ProductName" +
      " HAVING SUM(Quantity) >10 and SUM(Quantity) <100"

  val Q34_2: String = "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
      " FROM Order_Details od JOIN Products p ON" +
      " (p.ProductID = od.ProductID)" +
      " GROUP BY p.ProductName" +
      " HAVING SUM(Quantity) >100 and SUM(Quantity) <200"

  val Q35: String = "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City as employeeCity, c.City as customerCity" +
      " FROM Employees e JOIN Customers c ON" +
      " (e.City = c.City)" +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC"

  val Q35_1: String = "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City as employeeCity, c.City as customerCity" +
      " FROM Employees e JOIN Customers c ON" +
      " (e.City = c.City)" +
      " where e.EmployeeID > 5 " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC"

  val Q35_2: String = "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
      " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
      " e.City as employeeCity, c.City as customerCity" +
      " FROM Employees e JOIN Customers c ON" +
      " (e.City = c.City)" +
      " where e.EmployeeID > 1 " +
      " GROUP BY e.City, c.City " +
      " ORDER BY numEmployees DESC"

  val Q36: String = "select distinct (a.ShippedDate) as ShippedDate," +
      " a.OrderID," +
      " b.Subtotal," +
      " year(a.ShippedDate) as Year" +
      " from Orders a" +
      " inner join" +
      " (" +
      " select distinct OrderID," +
      " sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal" +
      " from order_details" +
      " group by OrderID" +
      " ) b on a.OrderID = b.OrderID" +
      " where a.ShippedDate is not null" +
      " and a.ShippedDate > Cast('1996-12-24' as TIMESTAMP) and " +
      " a.ShippedDate < Cast('1997-09-30' as TIMESTAMP)" +
      " order by ShippedDate"

  val Q36_1: String = "select distinct (a.ShippedDate) as ShippedDate," +
      " a.OrderID," +
      " b.Subtotal," +
      " year(a.ShippedDate) as Year" +
      " from Orders a" +
      " inner join" +
      " (" +
      " select distinct OrderID," +
      " sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal" +
      " from order_details" +
      " group by OrderID" +
      " ) b on a.OrderID = b.OrderID" +
      " where a.ShippedDate is not null" +
      " and a.ShippedDate > Cast('1997-02-24' as TIMESTAMP) and " +
      " a.ShippedDate < Cast('1997-09-30' as TIMESTAMP)" +
      " order by ShippedDate"

  val Q36_2: String = "select distinct (a.ShippedDate) as ShippedDate," +
      " a.OrderID," +
      " b.Subtotal," +
      " year(a.ShippedDate) as Year" +
      " from Orders a" +
      " inner join" +
      " (" +
      " select distinct OrderID," +
      " sum(UnitPrice * Quantity * (2 - Discount)) as Subtotal" +
      " from order_details" +
      " group by OrderID" +
      " ) b on a.OrderID = b.OrderID" +
      " where a.ShippedDate is not null" +
      " and a.ShippedDate > Cast('1996-02-24' as TIMESTAMP) and " +
      " a.ShippedDate < Cast('1996-09-30' as TIMESTAMP)" +
      " order by ShippedDate"

  val Q37: String = "select distinct a.CategoryID," +
      " a.CategoryName," +
      " b.ProductName," +
      " sum(c.ExtendedPrice) as ProductSales" +
      " from Categories a " +
      " inner join Products b on a.CategoryID = b.CategoryID" +
      " inner join" +
      " ( select distinct y.OrderID," +
      " y.ProductID," +
      " x.ProductName," +
      " y.UnitPrice," +
      " y.Quantity," +
      " y.Discount," +
      " round(y.UnitPrice * y.Quantity * (1 - y.Discount), 2) as ExtendedPrice" +
      " from Products x" +
      " inner join Order_Details y on x.ProductID = y.ProductID" +
      " order by y.OrderID" +
      " ) c on c.ProductID = b.ProductID" +
      " inner join Orders d on d.OrderID = c.OrderID" +
      " where d.OrderDate > Cast('1997-01-01' as TIMESTAMP) and " +
      "d.OrderDate < Cast('1997-12-31' as TIMESTAMP)" +
      " group by a.CategoryID, a.CategoryName, b.ProductName" +
      " order by a.CategoryName, b.ProductName, ProductSales"

  /*
    org.apache.spark.sql.AnalysisException: The correlated scalar subquery can only contain
    equality predicates: (UNITPRICE#976#1042 >= UNITPRICE#976);
    val Q38: String = "select distinct ProductName as Ten_Most_Expensive_Products," +
        " UnitPrice" +
        " from Products as a" +
        " where 10 >= (select count(distinct UnitPrice)" +
        " from Products as b" +
        " where b.UnitPrice == a.UnitPrice)" +
        "order by UnitPrice desc"
  */

  // A simple query to get detailed information for each sale so that invoice can be issued.
  val Q38: String = "select distinct b.ShipName," +
      " b.ShipAddress," +
      " b.ShipCity," +
      " b.ShipRegion," +
      " b.ShipPostalCode," +
      " b.ShipCountry," +
      " b.CustomerID," +
      " c.CompanyName as custCompanyName," +
      " c.Address," +
      " c.City," +
      " c.Region," +
      " c.PostalCode," +
      " c.Country, " +
      " concat(d.FirstName,  ' ', d.LastName) as Salesperson," +
      " b.OrderID," +
      " b.OrderDate," +
      " b.RequiredDate," +
      " b.ShippedDate," +
      " a.CompanyName as shippersCompanyName," +
      " e.ProductID," +
      " f.ProductName," +
      " e.UnitPrice," +
      " e.Quantity," +
      " e.Discount," +
      " e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice," +
      " b.Freight" +
      " from Shippers a " +
      " inner join Orders b on a.ShipperID = b.ShipVia" +
      " inner join Customers c on c.CustomerID = b.CustomerID" +
      " inner join Employees d on d.EmployeeID = b.EmployeeID" +
      " inner join Order_Details e on b.OrderID = e.OrderID" +
      " inner join Products f on f.ProductID = e.ProductID" +
      " order by b.ShipName"

  val Q38_1: String = "select distinct b.ShipName," +
      " b.ShipAddress," +
      " b.ShipCity," +
      " b.ShipRegion," +
      " b.ShipPostalCode," +
      " b.ShipCountry," +
      " b.CustomerID," +
      " c.CompanyName as custCompanyName," +
      " c.Address," +
      " c.City," +
      " c.Region," +
      " c.PostalCode," +
      " c.Country, " +
      " concat(d.FirstName,  ' ', d.LastName) as Salesperson," +
      " b.OrderID," +
      " b.OrderDate," +
      " b.RequiredDate," +
      " b.ShippedDate," +
      " a.CompanyName as shippersCompanyName," +
      " e.ProductID," +
      " f.ProductName," +
      " e.UnitPrice," +
      " e.Quantity," +
      " e.Discount," +
      " e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice," +
      " b.Freight" +
      " from Shippers a " +
      " inner join Orders b on a.ShipperID = b.ShipVia" +
      " inner join Customers c on c.CustomerID = b.CustomerID" +
      " inner join Employees d on d.EmployeeID = b.EmployeeID" +
      " inner join Order_Details e on b.OrderID = e.OrderID" +
      " inner join Products f on f.ProductID = e.ProductID" +
      " where b.ShippedDate > Cast('1996-07-10' as TIMESTAMP)" +
      " order by b.ShipName"

  val Q38_2: String = "select distinct b.ShipName," +
      " b.ShipAddress," +
      " b.ShipCity," +
      " b.ShipRegion," +
      " b.ShipPostalCode," +
      " b.ShipCountry," +
      " b.CustomerID," +
      " c.CompanyName as custCompanyName," +
      " c.Address," +
      " c.City," +
      " c.Region," +
      " c.PostalCode," +
      " c.Country, " +
      " concat(d.FirstName,  ' ', d.LastName) as Salesperson," +
      " b.OrderID," +
      " b.OrderDate," +
      " b.RequiredDate," +
      " b.ShippedDate," +
      " a.CompanyName as shippersCompanyName," +
      " e.ProductID," +
      " f.ProductName," +
      " e.UnitPrice," +
      " e.Quantity," +
      " e.Discount," +
      " e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice," +
      " b.Freight" +
      " from Shippers a " +
      " inner join Orders b on a.ShipperID = b.ShipVia" +
      " inner join Customers c on c.CustomerID = b.CustomerID" +
      " inner join Employees d on d.EmployeeID = b.EmployeeID" +
      " inner join Order_Details e on b.OrderID = e.OrderID" +
      " inner join Products f on f.ProductID = e.ProductID" +
      " where b.ShippedDate > Cast('1996-07-29' as TIMESTAMP)" +
      " order by b.ShipName"

  val Q39: String = "select s.supplierid,s.companyname,p.productid,p.productname " +
      "from suppliers s join products p on(s.supplierid= p.supplierid) and" +
      " s.companyname IN('Grandma Kellys Homestead','Tokyo Traders','Exotic Liquids')"

  val Q40: String = "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID"

  val Q40_1: String = "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID where c.CustomerID='LINOD'"

  val Q40_2: String = "SELECT c.customerID, o.orderID FROM customers c INNER JOIN orders o " +
      "ON c.CustomerID = o.CustomerID where c.CustomerID='SEVES'"

  val Q41: String = "SELECT order_details.OrderID,ShipCountry,UnitPrice,Quantity,Discount" +
      " FROM orders INNER JOIN Order_Details ON Orders.OrderID = Order_Details.OrderID"

  val Q42: String = "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID GROUP BY ShipCountry"

  val Q42_1: String = "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID where orders.OrderID > 11000 GROUP BY ShipCountry"

  val Q42_2: String = "SELECT ShipCountry," +
      " Sum(Order_Details.UnitPrice * Quantity * Discount)" +
      " AS ProductSales FROM Orders INNER JOIN Order_Details ON" +
      " Orders.OrderID = Order_Details.OrderID where orders.OrderID > 11070 GROUP BY ShipCountry"

  val Q43: String = "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId"

  val Q43_1: String = "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId where orders.OrderID > 11067"

  val Q43_2: String = "SELECT * FROM orders LEFT SEMI JOIN order_details " +
      "ON orders.OrderID = order_details.OrderId where orders.OrderID > 11075"

  val Q44: String = "SELECT * FROM orders LEFT SEMI JOIN order_details"

  val Q45: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders JOIN order_details"
  val Q46: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders LEFT JOIN order_details"
  val Q47: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders RIGHT JOIN order_details"
  val Q48: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders FULL OUTER JOIN order_details"
  val Q49: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders FULL JOIN order_details"
  val Q49_1: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate," +
      "RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders FULL JOIN order_details" +
      " where orders.ShippedDate > Cast('1996-07-29' as TIMESTAMP)"

  val Q49_2: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate," +
      "RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders FULL JOIN order_details" +
      " where orders.ShippedDate > Cast('1996-07-10' as TIMESTAMP)"

  val Q50: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"

  val Q51: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders LEFT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
  val Q51_1: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate," +
      "RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders LEFT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID " +
      " where orders.ShippedDate > Cast('1996-07-10' as TIMESTAMP)"

  val Q51_2: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate," +
      "RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders LEFT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID " +
      " where orders.ShippedDate > Cast('1996-07-29' as TIMESTAMP)"
  val Q52: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders RIGHT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
  val Q53: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders FULL OUTER JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
  val Q54: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      " ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode, " +
      "ShipCountry FROM orders FULL JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"

  // Number of units in stock by category and supplier continent
  val Q55: String = "select c.CategoryName as Product_Category," +
      " case when s.Country in" +
      " ('UK','Spain','Sweden','Germany','Norway'," +
      " 'Denmark','Netherlands','Finland','Italy','France')" +
      " then 'Europe'" +
      " when s.Country in ('USA','Canada','Brazil')" +
      " then 'America'" +
      " else 'Asia-Pacific'" +
      " end as Supplier_Continent," +
      " sum(p.UnitsInStock) as UnitsInStock" +
      " from Suppliers s " +
      " inner join Products p on p.SupplierID=s.SupplierID" +
      " inner join Categories c on c.CategoryID=p.CategoryID" +
      " group by c.CategoryName," +
      " case when s.Country in" +
      " ('UK','Spain','Sweden','Germany','Norway'," +
      " 'Denmark','Netherlands','Finland','Italy','France')" +
      " then 'Europe'" +
      " when s.Country in ('USA','Canada','Brazil')" +
      " then 'America'" +
      " else 'Asia-Pacific'" +
      " end"

  val Q55_1: String = "select c.CategoryName as Product_Category," +
      " case when s.Country in" +
      " ('UK','Spain','Sweden','Germany','Norway'," +
      " 'Denmark','Netherlands','Finland','Italy','France')" +
      " then 'Europe'" +
      " when s.Country in ('USA','Canada','Brazil')" +
      " then 'America'" +
      " else 'Asia-Pacific'" +
      " end as Supplier_Continent," +
      " sum(p.UnitsInStock) as UnitsInStock" +
      " from Suppliers s " +
      " inner join Products p on p.SupplierID=s.SupplierID" +
      " inner join Categories c on c.CategoryID=p.CategoryID" +
      " where s.Country IN ('USA','UK')" +
      " group by c.CategoryName," +
      " case when s.Country in" +
      " ('UK','Spain','Sweden','Germany','Norway'," +
      " 'Denmark','Netherlands','Finland','Italy','France')" +
      " then 'Europe'" +
      " when s.Country in ('USA','Canada','Brazil')" +
      " then 'America'" +
      " else 'Asia-Pacific'" +
      " end"

  val Q55_2: String = "select c.CategoryName as Product_Category," +
      " case when s.Country in" +
      " ('UK','Spain','Sweden','Germany','Norway'," +
      " 'Denmark','Netherlands','Finland','Italy','France')" +
      " then 'Europe'" +
      " when s.Country in ('USA','Canada','Brazil')" +
      " then 'America'" +
      " else 'Asia-Pacific'" +
      " end as Supplier_Continent," +
      " sum(p.UnitsInStock) as UnitsInStock" +
      " from Suppliers s " +
      " inner join Products p on p.SupplierID=s.SupplierID" +
      " inner join Categories c on c.CategoryID=p.CategoryID" +
      " where s.Country IN ('Canada','France')" +
      " group by c.CategoryName," +
      " case when s.Country in" +
      " ('UK','Spain','Sweden','Germany','Norway'," +
      " 'Denmark','Netherlands','Finland','Italy','France')" +
      " then 'Europe'" +
      " when s.Country in ('USA','Canada','Brazil')" +
      " then 'America'" +
      " else 'Asia-Pacific'" +
      " end"

  // This query shows sales figures by categories - mainly just aggregation with sub-query.
  // The inner query aggregates to product level, and the outer query further aggregates
  // the result set from inner-query to category level.
  val Q56: String = "select CategoryName, format_number(sum(ProductSales), 2) as CategorySales" +
      " from" +
      " (" +
      " select distinct a.CategoryName," +
      " b.ProductName," +
      " format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales," +
      " concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter" +
      " from Categories as a" +
      " inner join Products as b on a.CategoryID = b.CategoryID" +
      " inner join Order_Details as c on b.ProductID = c.ProductID" +
      " inner join Orders as d on d.OrderID = c.OrderID" +
      " where d.ShippedDate > Cast('1997-01-01' as TIMESTAMP) and " +
      "d.ShippedDate < Cast('1997-12-31' as TIMESTAMP)" +
      " group by a.CategoryName," +
      " b.ProductName," +
      " concat('Qtr ', quarter(d.ShippedDate))" +
      " order by a.CategoryName," +
      " b.ProductName," +
      " ShippedQuarter" +
      " ) as x" +
      " group by CategoryName" +
      " order by CategoryName"

  val Q56_1: String = "select CategoryName, format_number(sum(ProductSales), 2) as CategorySales" +
      " from" +
      " (" +
      " select distinct a.CategoryName," +
      " b.ProductName," +
      " format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales," +
      " concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter" +
      " from Categories as a" +
      " inner join Products as b on a.CategoryID = b.CategoryID" +
      " inner join Order_Details as c on b.ProductID = c.ProductID" +
      " inner join Orders as d on d.OrderID = c.OrderID" +
      " where d.ShippedDate > Cast('1996-12-01' as TIMESTAMP) and " +
      "d.ShippedDate < Cast('1997-07-10' as TIMESTAMP)" +
      " group by a.CategoryName," +
      " b.ProductName," +
      " concat('Qtr ', quarter(d.ShippedDate))" +
      " order by a.CategoryName," +
      " b.ProductName," +
      " ShippedQuarter" +
      " ) as x" +
      " group by CategoryName" +
      " order by CategoryName"

  val Q56_2: String = "select CategoryName, format_number(sum(ProductSales), 2) as CategorySales" +
      " from" +
      " (" +
      " select distinct a.CategoryName," +
      " b.ProductName," +
      " format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales," +
      " concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter" +
      " from Categories as a" +
      " inner join Products as b on a.CategoryID = b.CategoryID" +
      " inner join Order_Details as c on b.ProductID = c.ProductID" +
      " inner join Orders as d on d.OrderID = c.OrderID" +
      " where d.ShippedDate < Cast('1998-01-01' as TIMESTAMP) and " +
      "d.ShippedDate > Cast('1996-07-29' as TIMESTAMP)" +
      " group by a.CategoryName," +
      " b.ProductName," +
      " concat('Qtr ', quarter(d.ShippedDate))" +
      " order by a.CategoryName," +
      " b.ProductName," +
      " ShippedQuarter" +
      " ) as x" +
      " group by CategoryName" +
      " order by CategoryName"

  val Q56_3: String = "select CategoryName, format_number(sum(ProductSales), 2) as CategorySales" +
      " from" +
      " (" +
      " select distinct a.CategoryName," +
      " b.ProductName," +
      " format_number(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales," +
      " concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter" +
      " from Categories as a" +
      " inner join Products as b on a.CategoryID = b.CategoryID" +
      " inner join Order_Details as c on b.ProductID = c.ProductID" +
      " inner join Orders as d on d.OrderID = c.OrderID" +
      " where d.ShippedDate < Cast('1998-12-01' as TIMESTAMP) and " +
      "d.ShippedDate > Cast('1996-07-10' as TIMESTAMP)" +
      " group by a.CategoryName," +
      " b.ProductName," +
      " concat('Qtr ', quarter(d.ShippedDate))" +
      " order by a.CategoryName," +
      " b.ProductName," +
      " ShippedQuarter" +
      " ) as x" +
      " group by CategoryName" +
      " order by CategoryName"


  // This query shows how to use UNION to merge Customers and Suppliers into one result set by
  // identifying them as having different relationships to Northwind Traders - Customers and
  // Suppliers.

  val Q57: String = "select City, CompanyName, ContactName, 'Customers' as Relationship" +
      " from Customers" +
      " union" +
      " select City, CompanyName, ContactName, 'Suppliers'" +
      " from Suppliers" +
      " order by City, CompanyName"

  // In the query below, we have two sub-queries in the FROM clause and each sub-query returns a
  // single value. Because the results of the two sub-queries are basically temporary tables,
  // we can join them like joining two real tables.
  // In the SELECT clause, we simply list the two counts.

  val Q58: String = "select a.CustomersCount, b.SuppliersCount" +
      " from " +
      " (select count(CustomerID) as CustomersCount from customers) as a " +
      " join " +
      " (select count(SupplierID) as SuppliersCount from suppliers) as b"

  // The second query below uses the two values again but this time it calculates the ratio
  // between customers count and suppliers count. The round and concat function are used to
  // the result.

  val Q59: String = "select concat(round(a.CustomersCount / b.SuppliersCount), ':1') " +
      " as Customer_vs_Supplier_Ratio " +
      " from (select count(CustomerID) as CustomersCount from customers) as a " +
      " join (select count(SupplierID) as SuppliersCount from suppliers) as b"

  // This query shows how to convert order dates to the corresponding quarters. It also
  // demonstrates how SUM function is used together with CASE statement to get sales for each
  // quarter, where quarters are converted from OrderDate column.

  val Q60: String = "select a.ProductName," +
      " d.CompanyName," +
      " year(OrderDate) as OrderYear," +
      " format_number(sum(case quarter(c.OrderDate) when '1'" +
      " then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) as Qtr_1, " +
      " format_number(sum(case quarter(c.OrderDate) when '2'" +
      " then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) as Qtr_2, " +
      " format_number(sum(case quarter(c.OrderDate) when '3'" +
      " then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) as Qtr_3, " +
      " format_number(sum(case quarter(c.OrderDate) when '4'" +
      " then b.UnitPrice*b.Quantity*(1-b.Discount) else 0 end), 0) as Qtr_4 " +
      " from Products a" +
      " inner join Order_Details b on a.ProductID = b.ProductID" +
      " inner join Orders c on c.OrderID = b.OrderID" +
      " inner join Customers d on d.CustomerID = c.CustomerID" +
      " where c.OrderDate between Cast('1997-01-01' as TIMESTAMP) and Cast('1997-12-31' as  " +
      "TIMESTAMP)" +
      " group by a.ProductName," +
      " d.CompanyName," +
      " year(OrderDate)" +
      " order by a.ProductName, d.CompanyName"

  val Q61: String = "SELECT OrderDate, count(1) from Orders group by OrderDate order by  " +
      "OrderDate asc"

  val Q62: String = "SELECT OrderDate, count(1) from Orders group by OrderDate order by OrderDate"

  val queries = List(
    "Q1" -> Q1,
    "Q2" -> Q2,
    "Q3" -> Q3,
    "Q4" -> Q4,
    "Q5" -> Q5,
    "Q6" -> Q6,
    "Q7" -> Q7,
    "Q8" -> Q8,
    "Q9" -> Q9,
    "Q10" -> Q10,
    "Q11" -> Q11,
    "Q12" -> Q12,
    "Q13" -> Q13,
    "Q14" -> Q14,
    "Q15" -> Q15,
    "Q16" -> Q16,
    "Q17" -> Q17,
    "Q18" -> Q18,
    "Q19" -> Q19,
    "Q20" -> Q20,
    "Q21" -> Q21,
    "Q22" -> Q22,
    "Q23" -> Q23,
    "Q24" -> Q24,
    "Q25" -> Q25,
    "Q26" -> Q26,
    "Q27" -> Q27,
    "Q28" -> Q28,
    "Q29" -> Q29,
    "Q30" -> Q30,
    "Q61" -> Q61,
    "Q62" -> Q62,
    "Q31" -> Q31,
    "Q32" -> Q32,
    "Q33" -> Q33,
    "Q34" -> Q34,
    "Q35" -> Q35,
    "Q36" -> Q36,
    "Q37" -> Q37,
    "Q38" -> Q38,
    "Q39" -> Q39,
    "Q40" -> Q40,
    "Q41" -> Q41,
    "Q42" -> Q42,
    "Q43" -> Q43,
    "Q44" -> Q44,
    "Q45" -> Q45,
    "Q46" -> Q46,
    "Q47" -> Q47,
    "Q48" -> Q48,
    "Q49" -> Q49,
    "Q50" -> Q50,
    "Q51" -> Q51,
    "Q52" -> Q52,
    "Q53" -> Q53,
    "Q54" -> Q54,
    "Q55" -> Q55,
    "Q56" -> Q56,
    "Q57" -> Q57,
    "Q58" -> Q58,
    "Q59" -> Q59,
    "Q60" -> Q60
  )

  def regions(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark" +
      ".csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/regions.csv")

  val regions_table = "create table regions (" +
      "RegionID int, " +
      "RegionDescription string)"

  def categories(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/categories.csv")

  val categories_table = "create table categories (" +
      "CategoryID int, " +
      "CategoryName string, " +
      "Description string, " +
      "Picture blob)"

  def shippers(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark" +
      ".csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/shippers.csv")

  val shippers_table = "create table shippers (" +
      "ShipperID int not null, " +
      "CompanyName string not null, " +
      "Phone string)"

  def employees(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/employees.csv")

  val employees_table = "create table employees(" +
      //    "EmployeeID int not null , " +
      //    "LastName string not null, " +
      //    "FirstName string not null, " +
      "EmployeeID int, " +
      "LastName string,  " +
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
      "PhotoPath string)"

  def customers(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/customers.csv")

  val customers_table = "create table customers(" +
      //    "CustomerID string not null, " +
      //    "CompanyName string not null, " +
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
      "Fax string)"

  def orders(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/orders.csv")

  val orders_table = "create table orders (" +
      // "OrderID int not null, " +
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
      "ShipCountry string)"

  val large_orders_table = "create table orders (" +
      // "OrderID int not null, " +
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
      "ShipCountry string," +
      "bigComment string)"

  def order_details(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/order-details.csv")


  val order_details_table = "create table order_details (" +
      //    "OrderID int not null, " +
      //    "ProductID int not null, " +
      //    "UnitPrice double not null, " +
      //    "Quantity smallint not null, " +
      //    "Discount double not null)"
      "OrderID int, " +
      "ProductID int, " +
      "UnitPrice double, " +
      "Quantity smallint, " +
      "Discount double)"

  val large_order_details_table = "create table order_details (" +
      //    "OrderID int not null, " +
      //    "ProductID int not null, " +
      //    "UnitPrice double not null, " +
      //    "Quantity smallint not null, " +
      //    "Discount double not null)"
      "OrderID int, " +
      "ProductID int, " +
      "UnitPrice double, " +
      "Quantity smallint, " +
      "Discount double," +
      "bigComment string)"

  def products(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark" +
      ".csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/products.csv")

  val products_table = "create table products(" +
      // "ProductID int not null, " +
      "ProductID int, " +
      "ProductName string, " +
      //    "SupplierID int not null, " +
      //    "CategoryID int not null," +
      "SupplierID int, " +
      "CategoryID int," +
      "QuantityPerUnit string, " +
      "UnitPrice double, " +
      "UnitsInStock smallint, " +
      "UnitsOnOrder smallint," +
      "ReorderLevel smallint, " +
      "Discontinued smallint) "

  def suppliers(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/suppliers.csv")

  val suppliers_table = "create table suppliers(" +
      //    "SupplierID int not null, " +
      //    "CompanyName string not null, " +
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
      "HomePage string) "

  def territories(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/territories.csv")

  val territories_table = "create table territories(" +
      //    "TerritoryID string not null, " +
      //    "TerritoryDescription string not null, " +
      //    "RegionID string not null)"
      "TerritoryID string, " +
      "TerritoryDescription string, " +
      "RegionID string)"

  def employee_territories(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com" +
      ".databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/employee-territories.csv")

  val employee_territories_table = "create table employee_territories(" +
      //    "EmployeeID int not null, " +
      //    "TerritoryID int not null)"
      "EmployeeID int, " +
      // "TerritoryID int)"
      "TerritoryID string)"
}
