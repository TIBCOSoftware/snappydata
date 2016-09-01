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
package io.snappydata.hydra

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

object NWTestSnappyJob extends SnappySQLJob {
  var regions, categories, shippers, employees, customers, orders, order_details, products, suppliers, territories, employee_territories: DataFrame = null

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    snc.sql("set spark.sql.shuffle.partitions=6")
    NWQueries.snc = snc
    val dataLocation = jobConfig.getString("dataFilesLocation")
    println(s"SS - dataLocation is : ${dataLocation}")
    regions = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/regions.csv")
    categories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/categories.csv")
    shippers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/shippers.csv")
    employees = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/employees.csv")
    customers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/customers.csv")
    orders = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/orders.csv")
    order_details = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/order-details.csv")
    products = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/products.csv")
    suppliers = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/suppliers.csv")
    territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/territories.csv")
    employee_territories = snc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(s"$dataLocation/employee-territories.csv")
    val pw = new PrintWriter(new FileOutputStream(new File("NWTestSnappyJob.out"), true));
    dropTables(snc)
    println("Test replicated row tables queries started")
    createAndLoadReplicatedTables(snc)
    validateQueries(snc, "Replicated Row Table", pw)
    println("Test replicated row tables queries completed successfully")
    println("Test partitioned row tables queries started")
    createAndLoadPartitionedTables(snc)
    //    validatePartitionedRowTableQueries(snc)
    validateQueries(snc, "Partitioned Row Table", pw)
    println("Test partitioned row tables queries completed successfully")
    println("Test column tables queries started")
    createAndLoadColumnTables(snc)
    //    validatePartitionedColumnTableQueries(snc)
    validateQueries(snc, "Column Table", pw)
    println("Test column tables queries completed successfully")
    createAndLoadColocatedTables(snc)
    //    validateColocatedTableQueries(snc)
    validateQueries(snc, "Colocated Table", pw)
    pw.close()
  }

  private def assertJoin(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, tableType: String, pw: PrintWriter): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    pw.println(s"Query ${queryNum} \n df.count for join query is : ${df.count} \n Expected numRows : ${numRows} \n Table Type : ${tableType}")
    assert(df.count() == numRows,
      s"Mismatch got for query ${queryNum} : df.count ->" + df.count() + " but expected numRows ->" + numRows
        + " for query =" + sqlString + " Table Type : " + tableType)
  }

  private def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int, queryNum: String, tableType: String, pw: PrintWriter): Any = {
    val df = snc.sql(sqlString)
    pw.println(s"Query ${queryNum} \n df.count is : ${df.count} \n Expected numRows : ${numRows} \n Table Type : ${tableType}")
    assert(df.count() == numRows,
      s"Mismatch got for query ${queryNum} : df.count ->" + df.count() + " but expected numRows ->" + numRows
        + " for query =" + sqlString + " Table Type : " + tableType)
  }

  private def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {
    snc.sql(NWQueries.regions_table)
    regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table)
    orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table)
    order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table)
    products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table)
    suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table)
    territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table)
    employee_territories.write.insertInto("employee_territories")
  }

  private def validateQueries(snc: SnappyContext, tableType: String, pw: PrintWriter): Unit = {
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
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, "Q11", tableType, pw)
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, "Q12", tableType, pw)
        case "Q13" => assertQuery(snc, NWQueries.Q13, 0, "Q13", tableType, pw)
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, "Q14", tableType, pw)
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
        case "Q32" => assertJoin(snc, NWQueries.Q32, 29, "Q32", tableType, pw)
        case "Q33" => //assertJoin(snc, NWQueries.Q33, 51, "Q33")
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, "Q34", tableType, pw)
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, "Q35", tableType, pw)
        case "Q36" => assertJoin(snc, NWQueries.Q36, 5, "Q36", tableType, pw)
        case "Q37" => assertJoin(snc, NWQueries.Q37, 69, "Q37", tableType, pw)
        case "Q38" => assertJoin(snc, NWQueries.Q38, 71, "Q38", tableType, pw)
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
      }
    }
  }

  /*private def validateReplicatedTableQueries(snc: SnappyContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, "Q1")
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, "Q2")
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, "Q3")
        case "Q4" => assertQuery(snc, NWQueries.Q4, 8, "Q4")
        case "Q5" => assertQuery(snc, NWQueries.Q5, 7, "Q5")
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, "Q6")
        case "Q7" => assertQuery(snc, NWQueries.Q7, 7, "Q7")
        case "Q8" => assertQuery(snc, NWQueries.Q8, 5, "Q8")
        case "Q9" => assertQuery(snc, NWQueries.Q9, 2, "Q9")
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, "Q10")
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, "Q11")
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, "Q12")
        case "Q13" => assertQuery(snc, NWQueries.Q13, 0, "Q13")
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, "Q14")
        case "Q15" => assertQuery(snc, NWQueries.Q15, 3, "Q15")
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, "Q16")
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, "Q17")
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, "Q18")
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, "Q19")
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, "Q20")
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, "Q21")
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, "Q22")
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, "Q23")
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, "Q24")
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, "Q25")
        case "Q26" => assertJoin(snc, NWQueries.Q26, 89, "Q26")
        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, "Q27")
        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, "Q28")
        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, "Q29")
        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, "Q30")
        case "Q31" => assertJoin(snc, NWQueries.Q31, 683, "Q31")
        case "Q32" => assertJoin(snc, NWQueries.Q32, 51, "Q32")
        case "Q33" => //assertJoin(snc, NWQueries.Q33, 51, "Q33")
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, "Q34")
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, "Q35")
        case "Q36" => assertJoin(snc, NWQueries.Q36, 5, "Q36")
        case "Q37" => assertJoin(snc, NWQueries.Q37, 69, "Q37")
        case "Q38" => assertJoin(snc, NWQueries.Q38, 71, "Q38")
        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, "Q39")
        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, "Q40")
        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, "Q41")
        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, "Q42")
        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, "Q43")
        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, "Q44") //LeftSemiJoinHash
        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, "Q45")
        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, "Q46")
        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, "Q47")
        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, "Q48")
        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, "Q49")
        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, "Q50")
        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, "Q51")
        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, "Q52")
        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, "Q53")
        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, "Q54")
      }
    }
  }*/

  private def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using row options (partition_by 'OrderId', buckets '13')")
    orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
      " using row options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders')")
    order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
      " using row options ( partition_by 'ProductID', buckets '17')")
    products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
      " USING row options (PARTITION_BY 'SupplierID', buckets '123' )")
    suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
      " using row options (partition_by 'TerritoryID', buckets '3')")
    territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
      " using row options(partition_by 'EmployeeID', buckets '1')")
    employee_territories.write.insertInto("employee_territories")

  }

  /*private def validatePartitionedRowTableQueries(snc: SnappyContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, "Q1")
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, "Q2")
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, "Q3")
        case "Q4" => assertQuery(snc, NWQueries.Q4, 8, "Q4")
        case "Q5" => assertQuery(snc, NWQueries.Q5, 7, "Q5")
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, "Q6")
        case "Q7" => assertQuery(snc, NWQueries.Q7, 7, "Q7")
        case "Q8" => assertQuery(snc, NWQueries.Q8, 5, "Q8")
        case "Q9" => assertQuery(snc, NWQueries.Q9, 2, "Q9")
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, "Q10")
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, "Q11")
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, "Q12")
        case "Q13" => assertQuery(snc, NWQueries.Q13, 0, "Q13")
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, "Q14")
        case "Q15" => assertQuery(snc, NWQueries.Q15, 3, "Q15")
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, "Q16")
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, "Q17")
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, "Q18")
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, "Q19")
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, "Q20")
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, "Q21")
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, "Q22")
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, "Q23")
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, "Q24")
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, "Q25") //BroadcastLeftHashJOin
        case "Q26" => assertJoin(snc, NWQueries.Q26, 89, "Q26") //BroadcastLeftSemiJoinHash
        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, "Q27") //BroadcastLeftSemiJoinHash
        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, "Q28")
        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, "Q29") //BroadcastLeftSemiJoinHash
        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, "Q30")
        case "Q31" => assertJoin(snc, NWQueries.Q31, 683, "Q31")
        case "Q32" => assertJoin(snc, NWQueries.Q32, 51, "Q32")
        case "Q33" => //assertJoin(snc, NWQueries.Q33, 51, "Q33")
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, "Q34") //BroadcastHashJoin
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, "Q35")
        case "Q36" => assertJoin(snc, NWQueries.Q36, 5, "Q36")
        case "Q37" => assertJoin(snc, NWQueries.Q37, 69, "Q37")
        case "Q38" => assertJoin(snc, NWQueries.Q38, 71, "Q38")
        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, "Q39")
        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, "Q40")
        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, "Q41")
        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, "Q42")
        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, "Q43")
        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, "Q44") //LeftSemiJoinHash
        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, "Q45") //BroadcastLeftSemiJoinHash
        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, "Q46") //BroadcastLeftSemiJoinHash
        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, "Q47") //BroadcastLeftSemiJoinHash
        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, "Q48") //BroadcastLeftSemiJoinHash
        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, "Q49") //BroadcastLeftSemiJoinHash
        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, "Q50") //BroadcastHashJoin
        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, "Q51") //BroadcastHashOuterJoin
        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, "Q52") //BroadcastHashOuterJoin
        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, "Q53")
        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, "Q54")
      }
    }
  }*/

  private def createAndLoadColumnTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table + " using column options()")
    employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using column options (partition_by 'OrderId', buckets '13')")
    orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
      " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders')")
    order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
      " USING column options ( partition_by 'ProductID,SupplierID', buckets '17')")
    products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
      " USING column options (PARTITION_BY 'SupplierID', buckets '123' )")
    suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
      " using column options (partition_by 'TerritoryID', buckets '3')")
    territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
      " using row options(partition_by 'EmployeeID', buckets '1')")
    employee_territories.write.insertInto("employee_territories")
  }

  /*private def validatePartitionedColumnTableQueries(snc: SnappyContext): Unit = {

    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, "Q1")
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, "Q2")
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, "Q3")
        case "Q4" => assertQuery(snc, NWQueries.Q4, 8, "Q4")
        case "Q5" => assertQuery(snc, NWQueries.Q5, 7, "Q5")
        case "Q6" => assertQuery(snc, NWQueries.Q6, 8, "Q6")
        case "Q7" => assertQuery(snc, NWQueries.Q7, 7, "Q7")
        case "Q8" => assertQuery(snc, NWQueries.Q8, 5, "Q8")
        case "Q9" => assertQuery(snc, NWQueries.Q9, 2, "Q9")
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, "Q10")
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, "Q11")
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, "Q12")
        case "Q13" => assertQuery(snc, NWQueries.Q13, 0, "Q13")
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, "Q14")
        case "Q15" => assertQuery(snc, NWQueries.Q15, 3, "Q15")
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, "Q16")
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, "Q17")
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, "Q18")
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, "Q19")
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, "Q20")
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, "Q21")
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, "Q22")
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, "Q23")
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, "Q24")
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, "Q25") //BroadcastLeftHashJoin
        case "Q26" => assertJoin(snc, NWQueries.Q26, 89, "Q26") //BroadcastLeftSemiJoinHash
        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, "Q27") //BroadcastLeftSemiJoinHash
        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, "Q28")
        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, "Q29") //BroadcastLeftSemiJoinHash
        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, "Q30")
        case "Q31" => assertJoin(snc, NWQueries.Q31, 683, "Q31") //BroadcastHashJoin
        case "Q32" => assertJoin(snc, NWQueries.Q32, 51, "Q32")
        case "Q33" => //assertJoin(snc, NWQueries.Q33, 51, "Q33")
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, "Q34") //BroadcastHashJoin
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, "Q35")
        case "Q36" => assertJoin(snc, NWQueries.Q36, 5, "Q36")
        case "Q37" => assertJoin(snc, NWQueries.Q37, 69, "Q37") //SortMergeOuterJoin
        case "Q38" => assertJoin(snc, NWQueries.Q38, 71, "Q38")
        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, "Q39")
        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, "Q40")
        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, "Q41") //SortMergeJoin
        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, "Q42") // BroadcastHashJoin
        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, "Q43")
        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, "Q44")
        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, "Q45") //BroadcastLeftSemiJoinHash
        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, "Q46") //BroadcastLeftSemiJoinHash
        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, "Q47") // //BroadcastLeftSemiJoinHash
        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, "Q48") //BroadcastLeftSemiJoinHash //BroadcastNestedLoopJoin
        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, "Q49") //BroadcastLeftSemiJoinHash //BroadcastNestedLoopJoin
        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, "Q50") //BroadcastHashJoin
        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, "Q51") //BroadcastHashOuterJoin
        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, "Q52") //BroadcastHashOuterJoin
        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, "Q53")
        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, "Q54")
      }
    }
  }*/

  private def createAndLoadColocatedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table +
      " using row options( partition_by 'EmployeeID', buckets '3')")
    employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table +
      " using column options( partition_by 'CustomerID', buckets '19')")
    customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table +
      " using row options (partition_by 'CustomerID', buckets '19', colocate_with 'customers')")
    orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
      " using row options ( partition_by 'ProductID', buckets '329')")
    order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
      " USING column options ( partition_by 'ProductID', buckets '329'," +
      " colocate_with 'order_details')")
    products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
      " USING column options (PARTITION_BY 'SupplierID', buckets '123')")
    suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
      " using column options (partition_by 'TerritoryID', buckets '3')")
    territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
      " using row options(partition_by 'TerritoryID', buckets '3', colocate_with 'territories') ")
    employee_territories.write.insertInto("employee_territories")

  }


  /*private def validateColocatedTableQueries(snc: SnappyContext): Unit = {

    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, "Q1")
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, "Q2")
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, "Q3")
        case "Q4" => assertQuery(snc, NWQueries.Q4, 8, "Q4")
        case "Q5" => assertQuery(snc, NWQueries.Q5, 7, "Q5")
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, "Q6")
        case "Q7" => assertQuery(snc, NWQueries.Q7, 7, "Q7")
        case "Q8" => assertQuery(snc, NWQueries.Q8, 5, "Q8")
        case "Q9" => assertQuery(snc, NWQueries.Q9, 2, "Q9")
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, "Q10")
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, "Q11")
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, "Q12")
        case "Q13" => assertQuery(snc, NWQueries.Q13, 0, "Q13")
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, "Q14")
        case "Q15" => assertQuery(snc, NWQueries.Q15, 3, "Q15")
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, "Q16")
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, "Q17")
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, "Q18")
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, "Q19")
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, "Q20")
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, "Q21")
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, "Q22")
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, "Q23")
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, "Q24")
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, "Q25") //BroadcastLeftHashJOin
        case "Q26" => assertJoin(snc, NWQueries.Q26, 89, "Q26") //BroadcastLeftSemiJoinHash
        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, "Q27") //BroadcastLeftSemiJoinHash
        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, "Q28")
        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, "Q29") //BroadcastLeftSemiJoinHash
        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, "Q30")
        case "Q31" => assertJoin(snc, NWQueries.Q31, 683, "Q31")
        case "Q32" => assertJoin(snc, NWQueries.Q32, 51, "Q32")
        case "Q33" => //assertJoin(snc, NWQueries.Q33, 51, "Q33")
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, "Q34") //BroadcastHashJoin
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, "Q35") //SortMergeJoin
        case "Q36" => assertJoin(snc, NWQueries.Q36, 5, "Q36") //BroadcastHashOuterJoin
        case "Q37" => assertJoin(snc, NWQueries.Q37, 69, "Q37") //SortMergeOuterJoin
        case "Q38" => assertJoin(snc, NWQueries.Q38, 71, "Q38")
        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, "Q39")
        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, "Q40")
        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, "Q41")
        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, "Q42")
        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, "Q43")
        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, "Q44")
        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, "Q45")
        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, "Q46")
        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, "Q47")
        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, "Q48")
        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, "Q49")
        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, "Q50") //BroadcastHashJoin
        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, "Q51") //BroadcastHashOuterJoin
        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, "Q52") //BroadcastHashOuterJoin
        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, "Q53")
        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, "Q51")
      }
    }
  }*/

  private def dropTables(snc: SnappyContext): Unit = {
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

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()

}
