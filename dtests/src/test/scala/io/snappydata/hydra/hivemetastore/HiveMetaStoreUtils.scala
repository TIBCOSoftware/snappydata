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

  val setexternalHiveCatalog: String =
    "set spark.sql.catalogImplementation=hive"
  val setexternalInBuiltCatalog: String =
    "set spark.sql.catalogImplementation=in-memory"
  val showTblsApp: String =
    "show tables in app"
  val showTblsDefault: String =
    "show tables in default"
  val dropTable: String =
    "drop table if exists "
  val createDB: String =
    "create database "

  val joinHiveSnappy = new Array[String](4)
  val validateJoin = new Array[String](4)

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
  //  Query-3 takes > 55 minutes to execute not the validation.
  joinHiveSnappy(3) = "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
    " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
    " e.City as employeeCity, c.City as customerCity" +
    " FROM snappy_employees e JOIN default.hive_customers c ON" +
    " (e.City = c.City)" +
    " GROUP BY e.City, c.City " +
    " ORDER BY numEmployees DESC"

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
  validateJoin(3) = "SELECT COUNT(DISTINCT e.EmployeeID) AS numEmployees," +
    " COUNT(DISTINCT c.CustomerID) AS numCompanies," +
    " e.City as employeeCity, c.City as customerCity" +
    " FROM snappy_employees e JOIN snappy_customers c ON" +
    " (e.City = c.City)" +
    " GROUP BY e.City, c.City " +
    " ORDER BY numEmployees DESC"
}
