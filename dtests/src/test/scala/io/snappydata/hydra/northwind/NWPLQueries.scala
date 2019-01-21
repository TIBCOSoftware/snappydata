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
package io.snappydata.hydra.northwind


object NWPLQueries {
  // var snc: SnappyContext = _
  val Q1: String = "SELECT * FROM Orders WHERE OrderID = 10506"
  val Q2: String = "SELECT EmployeeID, CustomerID FROM Orders WHERE OrderID = 10267"
  val Q3: String = "SELECT EmployeeID,OrderDate,RequiredDate,ShippedDate FROM Orders WHERE  " +
      "OrderID = 10276"
  val Q4: String = "SELECT ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,ShipCountry " +
      "FROM Orders WHERE OrderID = 10306"
  val Q5: String = "SELECT OrderID, Freight FROM Orders WHERE OrderID = 10338"
  val Q6: String = "SELECT Freight * 1.1 AS FreightTotal FROM Orders WHERE OrderID = 10386"
  val Q7: String = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders WHERE  " +
      "OrderID = 10494"
  val Q8: String = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders WHERE  " +
      "OrderID = 10570"
  val Q9: String = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders WHERE  " +
      "OrderID = 10619"
  val Q10: String = "SELECT OrderID, Freight, Freight * 1.1 AS FreightTotal FROM Orders WHERE  " +
      "OrderID = 10680"
  val Q11: String = "SELECT CustomerID FROM Orders WHERE OrderID = 10290"
  val Q12: String = "SELECT SUM(Quantity) AS TotalUnits FROM Order_Details WHERE OrderID = 10340"
  val Q13: String = "SELECT ProductID, UnitPrice FROM Order_Details WHERE OrderID = 10260"
  val Q14: String = "SELECT AVG(UnitPrice) AS AveragePrice FROM Order_Details WHERE OrderID = 10280"
  val Q15: String = "SELECT UnitPrice,Quantity,Discount FROM Order_Details WHERE OrderID = 10306"
  val Q16: String = "SELECT UnitPrice FROM Order_Details WHERE OrderID = 10323"
  val Q17: String = "SELECT Quantity FROM Order_Details WHERE OrderID = 10357"
  val Q18: String = "SELECT Discount FROM Order_Details WHERE OrderID = 10375"
  val Q19: String = "SELECT Sum(UnitPrice * Quantity * Discount) AS ProductSales  FROM  " +
      "Order_Details WHERE OrderID = 10411"
  val Q20: String = "SELECT sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal FROM  " +
      "Order_Details WHERE OrderID = 10451"
  val Q21: String = "SELECT distinct OrderID FROM Order_Details WHERE OrderID = 11077"
  val Q22: String = "SELECT OrderID,ProductID,UnitPrice,Quantity,Discount FROM Order_Details  " +
      "WHERE OrderID = 11039"
  val Q23: String = "SELECT * FROM Order_Details WHERE OrderID = 10995"
  val Q24: String = "SELECT OrderID, ProductID, Quantity, UnitPrice, UnitPrice*Quantity FROM  " +
      "order_details WHERE OrderID = 10975"
  val Q25: String = "SELECT distinct sum(UnitPrice * Quantity * (1 - Discount)) as Subtotal from " +
      " order_details where OrderID=10871"
  val Q26: String = "SELECT LastName,FirstName FROM Employees where (City='London' AND " +
      "Country='UK' )"
  val Q27: String = "SELECT EmployeeID,LastName,FirstName,Title FROM Employees where " +
      "(City='Seattle' AND Country='USA')"
  val Q28: String = "SELECT LastName,FirstName,Cast(BirthDate  as TIMESTAMP),HireDate,Address " +
      "FROM Employees where (City='Tacoma' AND Country='USA')"
  val Q29: String = "SELECT TitleOfCourtesy, FirstName, LastName FROM Employees where  " +
      "(City='Kirkland' AND Country='USA')"
  val Q30: String = "SELECT CONCAT(FirstName, ' ', LastName) FROM Employees where  " +
      "(City='Redmond' AND Country='USA')"
  val Q31: String = "SELECT FirstName, LastName, Cast(BirthDate as TIMESTAMP) FROM Employees " +
      "where (City='Kirkland' AND Country='USA')"
  val Q32: String = "SELECT COUNT(DISTINCT City) AS NumCities FROM Employees where  " +
      "(City='London' AND Country='UK')"
  val Q33: String = "SELECT COUNT(DISTINCT EmployeeID) AS numEmployees FROM Employees where " +
      "(City='London' AND Country='UK')"
  val Q34: String = "SELECT COUNT(EmployeeID) AS totalEmployees FROM Employees where " +
      "(City='London' AND Country='UK')"
  val Q35: String = "SELECT LastName,FirstName,HomePhone,Extension FROM Employees where " +
      "(City='Seattle' AND Country='USA')"
  val Q36: String = "SELECT EmployeeID,LastName,FirstName,Title,TitleOfCourtesy, Cast(BirthDate " +
      "as TIMESTAMP), Cast(HireDate as TIMESTAMP),Address,City,Region,PostalCode,Country," +
      "HomePhone, Extension,Notes,ReportsTo,PhotoPath FROM Employees  where (City='London' AND " +
      "Country='UK')"
  val Q37: String = "SELECT LastName,FirstName,Address,City,Region,PostalCode FROM Employees " +
      "where (City='London' AND Country='UK')"
  val Q38: String = "SELECT SupplierID FROM Products WHERE (ProductID=14 AND SupplierID=6)"
  val Q39: String = "SELECT ProductName, UnitsInStock, ReorderLevel FROM Products WHERE " +
      "(ProductID = 75 AND SupplierID = 12)"
  val Q40: String = "SELECT EmployeeID,LastName,FirstName,Title,TitleOfCourtesy, Cast(BirthDate " +
      "as TIMESTAMP),Cast(HireDate as TIMESTAMP), Address,City,Region,PostalCode,Country," +
      "HomePhone,Extension,Notes,ReportsTo,PhotoPath from Employees  where (City='London' AND " +
      "Country='UK')"
  val Q41: String = "SELECT * from Customers  where (City='London' AND Country='UK')"
  val Q42: String = "SELECT CustomerID,CompanyName,ContactName,ContactTitle,Address from " +
      "Customers  where (City='Berlin' AND Country='Germany')"
  // scalastyle:off
  val Q43: String = "SELECT CustomerID,City,Region,PostalCode from Customers  " +
      "where (City='México D.F.' AND Country='Mexico')"
  val Q44: String = "SELECT CustomerID,ContactName,ContactTitle from Customers  " +
      "where (City='Luleå' AND Country='Sweden')"
  // scalastyle:on
  val Q45: String = "SELECT ContactName,CompanyName,City,Country,Region from Customers  where " +
      "(City='Strasbourg' AND Country='France')"
  val Q46: String = "SELECT CustomerID As CustomerID_London from Customers  where  " +
      "(City='Tsawassen' AND Country='Canada')"
  val Q47: String = "SELECT SupplierID,CompanyName from suppliers where SupplierID=25"
  val Q48: String = "SELECT * from suppliers where SupplierID=25"
  val Q49: String = "SELECT ContactName,ContactTitle,Address from suppliers where SupplierID=29"
  val Q50: String = "SELECT SupplierID,CompanyName from suppliers where SupplierID=20"
  val Q51: String = "SELECT RegionID,TerritoryDescription from territories where TerritoryID=44122"
  val Q52: String = "SELECT ProductName, SupplierID FROM Products WHERE (SupplierID IN (SELECT " +
      "SupplierID FROM Suppliers WHERE CompanyName IN  ('Exotic Liquids', 'Grandma Kelly''s " +
      "Homestead', 'Tokyo Traders')) AND ProductID=14)"

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
    "Q51" -> Q51
    // "Q52" -> Q52
  )
}
