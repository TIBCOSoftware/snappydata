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
package io.snappydata.hydra.northWind

import java.io.PrintWriter

import org.apache.spark.sql.SnappyContext

object NWTestUtil {

  def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, tableType: String, pw: PrintWriter): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    pw.println(s"Query ${queryNum} \n df.count for join query is : ${df.count} \n Expected numRows : ${numRows} \n Table Type : ${tableType}")
    println(s"Query ${queryNum} \n df.count for join query is : ${df.count} \n Expected numRows : ${numRows} \n Table Type : ${tableType}")
    println(df.show(100))
    pw.println(df.show(100))
    assert(df.count() == numRows,
      s"Mismatch got for query ${queryNum} : df.count ->" + df.count() + " but expected numRows ->" + numRows
        + " for query =" + sqlString + " Table Type : " + tableType)
  }

  def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, tableType: String, pw: PrintWriter): Any = {
    val df = snc.sql(sqlString)
    pw.println(s"Query ${queryNum} \n df.count is : ${df.count} \n Expected numRows : ${numRows} \n Table Type : ${tableType}")
    println(s"Query ${queryNum} \n df.count is : ${df.count} \n Expected numRows : ${numRows} \n Table Type : ${tableType}")
    println(df.show(100))
    pw.println(df.show(100))
    assert(df.count() == numRows,
      s"Mismatch got for query ${queryNum} : df.count ->" + df.count() + " but expected numRows ->" + numRows
        + " for query =" + sqlString + " Table Type : " + tableType)
  }

  def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    NWQueries.employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    NWQueries.customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table)
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table)
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table)
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table)
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table)
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table)
    NWQueries.employee_territories.write.insertInto("employee_territories")
  }

  def validateQueries(snc: SnappyContext, tableType: String, pw: PrintWriter): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, "Q1", tableType, pw)
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, "Q2", tableType, pw)
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, "Q3", tableType, pw)
        case "Q4" => assertQuery(snc, NWQueries.Q4, 9, "Q4", tableType, pw)
        case "Q5" => assertQuery(snc, NWQueries.Q5, 9, "Q5", tableType, pw)
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, "Q6", tableType, pw)
        case "Q7" => assertQuery(snc, NWQueries.Q7, 9, "Q7", tableType, pw)
        case "Q8" => assertQuery(snc, NWQueries.Q8, 6, "Q8", tableType, pw)
        case "Q9" => assertQuery(snc, NWQueries.Q9, 3, "Q9", tableType, pw)
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, "Q10", tableType, pw)
        case "Q11" => assertQuery(snc, NWQueries.Q11, 4, "Q11", tableType, pw)
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, "Q12", tableType, pw)
        case "Q13" => assertQuery(snc, NWQueries.Q13, 2, "Q13", tableType, pw)
        case "Q14" => assertQuery(snc, NWQueries.Q14, 69, "Q14", tableType, pw)
        case "Q15" => assertQuery(snc, NWQueries.Q15, 5, "Q15", tableType, pw)
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, "Q16", tableType, pw)
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, "Q17", tableType, pw)
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, "Q18", tableType, pw)
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, "Q19", tableType, pw)
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, "Q20", tableType, pw)
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, "Q21", tableType, pw)
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, "Q22", tableType, pw)
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, "Q23", tableType, pw)
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, "Q24", tableType, pw)
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, "Q25", tableType, pw)
        case "Q26" => assertJoin(snc, NWQueries.Q26, 86, "Q26", tableType, pw)
        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, "Q27", tableType, pw)
        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, "Q28", tableType, pw)
        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, "Q29", tableType, pw)
        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, "Q30", tableType, pw)
        case "Q31" => assertJoin(snc, NWQueries.Q31, 830, "Q31", tableType, pw)
        case "Q32" => assertJoin(snc, NWQueries.Q32, 8, "Q32", tableType, pw)
        case "Q33" => //assertJoin(snc, NWQueries.Q33, 51, "Q33", tableType, pw)
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, "Q34", tableType, pw)
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, "Q35", tableType, pw)
        case "Q36" => assertJoin(snc, NWQueries.Q36, 290, "Q36", tableType, pw)
        case "Q37" => assertJoin(snc, NWQueries.Q37, 77, "Q37", tableType, pw)
        //case "Q38" => assertJoin(snc, NWQueries.Q38, 2155, "Q38", tableType, pw) // NPE LocalJoin
        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, "Q39", tableType, pw)
        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, "Q40", tableType, pw)
        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, "Q41", tableType, pw)
        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, "Q42", tableType, pw)
        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, "Q43", tableType, pw)
        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, "Q44", tableType, pw) //LeftSemiJoinHash
        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, "Q45", tableType, pw)
        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, "Q46", tableType, pw)
        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, "Q47", tableType, pw)
        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, "Q48", tableType, pw)
        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, "Q49", tableType, pw)
        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, "Q50", tableType, pw)
        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, "Q51", tableType, pw)
        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, "Q52", tableType, pw)
        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, "Q53", tableType, pw)
        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, "Q54", tableType, pw)
        case "Q55" => assertJoin(snc, NWQueries.Q55, 21, "Q55", tableType, pw)
        case "Q56" => assertJoin(snc, NWQueries.Q56, 8, "Q56", tableType, pw)
        case _ => println("OK")
      }
    }
  }

  def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table + " using row options(partition_by 'PostalCode,Region', buckets '19', redundancy '1')")
    NWQueries.employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table +
      " using row options( partition_by 'PostalCode,Region', buckets '19', colocate_with 'employees', redundancy '1')")
    NWQueries.customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using row options (partition_by 'OrderId', buckets '13', redundancy '1')")
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
      " using row options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', redundancy '1')")
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
      " using row options ( partition_by 'ProductID,SupplierID', buckets '17', redundancy '1')")
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
      " USING row options (PARTITION_BY 'SupplierID', buckets '123',redundancy '1')")
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
      " using row options (partition_by 'TerritoryID', buckets '3', redundancy '1')")
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
      " using row options(partition_by 'EmployeeID', buckets '1', redundancy '1')")
    NWQueries.employee_territories.write.insertInto("employee_territories")

  }

  def createAndLoadColumnTables(snc: SnappyContext): Unit = {
    snc.sql(NWQueries.regions_table)
    NWQueries.regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table + " using row options(partition_by 'City,Country', redundancy '1')")
    NWQueries.employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table + " using column options(partition_by 'City,Country', COLOCATE_WITH 'employees', redundancy '1')")
    NWQueries.customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using column options (partition_by 'OrderId', buckets '13', redundancy '1')")
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
      " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders', redundancy '1')")
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
      " USING column options (partition_by 'ProductID,SupplierID', buckets '17', redundancy '1')")
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
      " USING column options (PARTITION_BY 'SupplierID', buckets '123', redundancy '1')")
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
      " using column options (partition_by 'TerritoryID', buckets '3', redundancy '1')")
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
      " using row options(partition_by 'EmployeeID', buckets '1', redundancy '1')")
    NWQueries.employee_territories.write.insertInto("employee_territories")
  }

  def createAndLoadColocatedTables(snc: SnappyContext): Unit = {
    snc.sql(NWQueries.regions_table)
    NWQueries.regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table +
      " using row options( partition_by 'PostalCode,Region', buckets '19', redundancy '1')")
    NWQueries.employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table +
      " using column options( partition_by 'PostalCode,Region', buckets '19', colocate_with 'employees', redundancy '1')")
    NWQueries.customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table +
      " using row options (partition_by 'CustomerID, OrderID', buckets '19', redundancy '1')")
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
      " using row options ( partition_by 'ProductID', buckets '329', redundancy '1')")
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
      " USING column options ( partition_by 'ProductID', buckets '329'," +
      " colocate_with 'order_details', redundancy '1')")
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
      " USING column options (PARTITION_BY 'SupplierID', buckets '123', redundancy '1')")
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
      " using column options (partition_by 'TerritoryID', buckets '3', redundancy '1')")
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
      " using row options(partition_by 'TerritoryID', buckets '3', colocate_with 'territories', redundancy '1') ")
    NWQueries.employee_territories.write.insertInto("employee_territories")
  }


  def dropTables(snc: SnappyContext): Unit = {
    snc.sql("drop table if exists regions")
    println("regions table dropped successfully.");
    snc.sql("drop table if exists categories")
    println("categories table dropped successfully.");
    snc.sql("drop table if exists products")
    println("products table dropped successfully.");
    snc.sql("drop table if exists order_details")
    println("order_details table dropped successfully.");
    snc.sql("drop table if exists orders")
    println("orders table dropped successfully.");
    snc.sql("drop table if exists customers")
    println("customers table dropped successfully.");
    snc.sql("drop table if exists employees")
    println("employees table dropped successfully.");
    snc.sql("drop table if exists employee_territories")
    println("employee_territories table dropped successfully.");
    snc.sql("drop table if exists shippers")
    println("shippers table dropped successfully.");
    snc.sql("drop table if exists suppliers")
    println("suppliers table dropped successfully.");
    snc.sql("drop table if exists territories")
    println("territories table dropped successfully.");
  }

}
