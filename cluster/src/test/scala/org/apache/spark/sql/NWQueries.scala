/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import org.apache.spark.sql.execution._

object NWQueries extends SnappyFunSuite {

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
      " WHERE OrderDate = '1997-05-19'"

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
      " WHERE BirthDate BETWEEN '1950-01-01' AND '1959-12-31 23:59:59'"

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
      "FROM Orders WHERE OrderDate BETWEEN '1997-01-01' AND '1997-12-31')"

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
      " WHERE o.ShippedDate > o.RequiredDate AND o.OrderDate > '1998-01-01'" +
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
      " and a.ShippedDate > '1996-12-24' and a.ShippedDate < '1997-09-30'" +
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
      " where d.OrderDate > '1997-01-01' and d.OrderDate < '1997-12-31'" +
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
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders JOIN order_details"
  val Q46: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders LEFT JOIN order_details"
  val Q47: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders RIGHT JOIN order_details"
  val Q48: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders FULL OUTER JOIN order_details"
  val Q49: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
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
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"

  val Q51: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
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
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders RIGHT JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
  val Q53: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
      "ShipCountry FROM orders FULL OUTER JOIN order_details" +
      " ON Orders.OrderID = Order_Details.OrderID"
  val Q54: String = "SELECT orders.OrderID as OOID, CustomerID,EmployeeID,OrderDate,RequiredDate," +
      "ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode," +
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
      " where d.ShippedDate > '1997-01-01' and d.ShippedDate < '1997-12-31'" +
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
      " where d.ShippedDate < Cast('1997-12-01' as TIMESTAMP) and " +
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
    "Q25_1" -> Q25_1,
    "Q25_2" -> Q25_2,
    "Q26" -> Q26,
    "Q26_1" -> Q26_1,
    "Q26_2" -> Q26_2,
    "Q27" -> Q27,
    "Q27_1" -> Q27_1,
    "Q27_2" -> Q27_2,
    "Q27_3" -> Q27_3,
    "Q27_4" -> Q27_4,
    "Q28" -> Q28,
    "Q28_1" -> Q28_1,
    "Q28_2" -> Q28_2,
    "Q29" -> Q29,
    "Q29_1" -> Q29_1,
    "Q29_2" -> Q29_2,
    "Q30" -> Q30,
    "Q30_1" -> Q30_1,
    "Q30_2" -> Q30_2,
    "Q31" -> Q31,
    "Q31_1" -> Q31_1,
    "Q31_2" -> Q31_2,
    "Q31_3" -> Q31_3,
    "Q31_4" -> Q31_4,
    "Q32" -> Q32,
    "Q32_1" -> Q32_1,
    "Q33" -> Q33,
    "Q33_1" -> Q33_1,
    "Q34" -> Q34,
    "Q34_1" -> Q34_1,
    "Q34_2" -> Q34_2,
    "Q35" -> Q35,
    "Q35_1" -> Q35_1,
    "Q35_2" -> Q35_2,
    "Q36" -> Q36,
    "Q36_1" -> Q36_1,
    "Q36_2" -> Q36_2,
    "Q37" -> Q37,
    "Q38" -> Q38,
    "Q38_1" -> Q38_1,
    "Q38_2" -> Q38_2,
    "Q39" -> Q39,
    "Q40" -> Q40,
    "Q40_1" -> Q40_1,
    "Q40_2" -> Q40_2,
    "Q41" -> Q41,
    "Q42" -> Q42,
    "Q42_1" -> Q42_1,
    "Q42_2" -> Q42_2,
    "Q43" -> Q43,
    "Q43_1" -> Q43_1,
    "Q43_2" -> Q43_2,
    "Q44" -> Q44,
    "Q45" -> Q45,
    "Q46" -> Q46,
    "Q47" -> Q47,
    "Q48" -> Q48,
    "Q49" -> Q49,
    "Q49_1" -> Q49_1,
    "Q49_2" -> Q49_2,
    "Q50" -> Q50,
    "Q51" -> Q51,
    "Q51_1" -> Q51_1,
    "Q51_2" -> Q51_2,
    "Q52" -> Q52,
    "Q53" -> Q53,
    "Q54" -> Q54,
    "Q55" -> Q55,
    "Q55_1" -> Q55_1,
    "Q55_2" -> Q55_2,
    "Q56" -> Q56,
    "Q56_1" -> Q56_1,
    "Q56_2" -> Q56_2,
    "Q56_3" -> Q56_3
  )

  def regions(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/regions.csv").getPath))

  val regions_table = "create table regions (" +
      "RegionID int, " +
      "RegionDescription string)"

  def categories(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/categories.csv").getPath))

  val categories_table = "create table categories (" +
      "CategoryID int, " +
      "CategoryName string, " +
      "Description string, " +
      "Picture blob)"

  def shippers(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/shippers.csv").getPath))

  val shippers_table = "create table shippers (" +
      "ShipperID int not null, " +
      "CompanyName string not null, " +
      "Phone string)"

  def employees(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/employees.csv").getPath))

  val employees_table = "create table employees(" +
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

  def customers(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/customers.csv").getPath))

  val customers_table = "create table customers(" +
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

  def orders(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/orders.csv").getPath))

  val orders_table = "create table orders (" +
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

  def order_details(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("nullValue", "NULL")
    .option("maxCharsPerColumn", "4096")
    .csv((getClass.getResource("/northwind/order-details.csv").getPath))

  val order_details_table = "create table order_details (" +
      "OrderID int, " +
      "ProductID int, " +
      "UnitPrice double, " +
      "Quantity smallint, " +
      "Discount double)"

  def products(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/products.csv").getPath))

  val products_table = "create table products(" +
      // "ProductID int not null, " +
      "ProductID int, " +
      "ProductName string, " +
      "SupplierID int, " +
      "CategoryID int," +
      "QuantityPerUnit string, " +
      "UnitPrice double, " +
      "UnitsInStock smallint, " +
      "UnitsOnOrder smallint," +
      "ReorderLevel smallint, " +
      "Discontinued smallint) "

  def suppliers(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/suppliers.csv").getPath))

  val suppliers_table = "create table suppliers(" +
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

  def territories(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/territories.csv").getPath))

  val territories_table = "create table territories(" +
      "TerritoryID string, " +
      "TerritoryDescription string, " +
      "RegionID string)"

  def employee_territories(sqlContext: SQLContext): DataFrame =
    sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("maxCharsPerColumn", "4096")
    .option("nullValue", "NULL")
    .csv((getClass.getResource("/northwind/employee-territories.csv").getPath))

  val employee_territories_table = "create table employee_territories(" +
      "EmployeeID int, " +
      "TerritoryID string)"

  def dropTables(snc: SnappyContext): Unit = {
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

  /**
   * Enable this flag for local testing in case a change causes multiple cases
   * of mismatches to be introduced.
   */
  private val WARN_FOR_PARTITION_MISMATCH = false

  def assertJoin(snc: SnappyContext, sqlString: String, queryNum: String, numRows: Int,
      numPartitions: Int, c: Class[_]): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    val count = df.count()
    assert(count == numRows,
      "Mismatch got df.count -> " + count + " but expected numRows -> "
          + numRows + " for queryNum = " + queryNum)
    val expectedPartitions = (numPartitions - 4) to (numPartitions + 4)
    if (!expectedPartitions.contains(df.rdd.partitions.length)) {
      logWarning("Mismatch got df.rdd.partitions.length -> " + df.rdd.partitions.length +
          " but expected numPartitions -> " + numPartitions +
          " for queryNum = " + queryNum)
    }
  }

  private def assertQueryCommon(df: DataFrame, sqlString: String,
      queryNum: String, numRows: Int, c: Class[_]): Any = {
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: ProjectExec => j
      case j: PartitionedDataSourceScan => j
      case j: PartitionedPhysicalScan => j
      case j: LocalTableScanExec => j
      case j: CoalesceExec => j
      case j: FilterExec => j
      case j: OutputFakerExec => j
      case j: RangeExec => j
      case j: SampleExec => j
      case j: SubqueryExec => j
      case j: UnionExec => j
    }
    if (operators.head.getClass != c) {
      throw new IllegalStateException(s"$sqlString expected operator: $c," +
          s" but got ${operators.head}\n physical: \n$physical")
    }
    val count = df.count()
    assert(count == numRows,
      "Mismatch got df.count -> " + count + " but expected numRows -> " +
          numRows + " for queryNum = " + queryNum)
  }

  def assertQuery(snc: SnappyContext, sqlString: String, queryNum: String,
      numRows: Int, numPartitions: Int, c: Class[_]): Any = {
    val df = snc.sql(sqlString)
    assertQueryCommon(df, sqlString, queryNum, numRows, c)

    if (WARN_FOR_PARTITION_MISMATCH) {
      if (df.rdd.partitions.length != numPartitions) {
        logWarning("Mismatch got df.rdd.partitions.length -> " + df.rdd.partitions.length +
            " but expected numPartitions -> " + numPartitions +
            " for queryNum = " + queryNum)
      }
    } else {
      assert(df.rdd.partitions.length == numPartitions,
        "Mismatch got df.rdd.partitions.length -> " + df.rdd.partitions.length +
            " but expected numPartitions -> " + numPartitions +
            " for queryNum = " + queryNum)
    }
  }

  def assertQuery(snc: SnappyContext, sqlString: String, queryNum: String,
      numRows: Int, numPartitions: Array[Int], c: Class[_]): Any = {
    val df = snc.sql(sqlString)
    assertQueryCommon(df, sqlString, queryNum, numRows, c)

    val rddNumPartitions = df.rdd.partitions.length
    if (WARN_FOR_PARTITION_MISMATCH) {
      if (!numPartitions.contains(rddNumPartitions)) {
        logWarning("Mismatch got df.rdd.partitions.length -> " + rddNumPartitions +
            " but expected one of numPartitions -> " + numPartitions.toSeq +
            " for queryNum=" + queryNum)
      }
    } else {
      assert(numPartitions.contains(rddNumPartitions),
        "Mismatch got df.rdd.partitions.length -> " + rddNumPartitions +
            " but expected one of numPartitions -> " + numPartitions.toSeq +
            " for queryNum=" + queryNum)
    }
  }
}
