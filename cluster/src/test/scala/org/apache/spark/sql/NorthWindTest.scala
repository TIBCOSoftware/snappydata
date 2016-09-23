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
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.joins._

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
    snc.sql("set spark.sql.crossJoin.enabled = true")
    val df = snc.sql(sqlString)
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: LocalJoin => j
      // case j: LeftSemiJoinHashExec => j
      case j: BroadcastHashJoinExec => j
      // case j: BroadcastHashOuterJoinExec => j
      case j: BroadcastNestedLoopJoinExec => j
      // case j: BroadcastLeftSemiJoinHash => j
      // case j: LeftSemiJoinBNL => j
      case j: CartesianProductExec => j
      case j: SortMergeJoinExec => j
      case j: ShuffledHashJoinExec => j
      case j: PartitionedPhysicalScan => j

    }
    if (operators(0).getClass() != c) {
      throw new IllegalStateException(s"$sqlString expected operator: $c," +
          s" but got ${operators(0)}\n physical: \n$physical")
    }
    assert(df.count() == numRows,
      "Mismatch got df.count ->" + df.count() + " but expected numRows ->"
          + numRows + " for query " + sqlString)
    assert(df.rdd.partitions.length == numPartitions,
      "Mismatch got df.rdd.partitions.length ->" + df.rdd.partitions.length +
          " but expected numPartitions ->" + numPartitions + " for query " + sqlString)
  }

  private def assertQuery(snc: SnappyContext, sqlString: String, numRows: Int,
      numPartitions: Int, c: Class[_]): Any = {
    val df = snc.sql(sqlString)
    // df.explain()
    val physical = df.queryExecution.sparkPlan
    val operators = physical.collect {
      // case j: SortBasedAggregate => j
      // case j: Sort => j
      case j: ProjectExec => j
      // case j: TungstenAggregate => j
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
   // println("********" + operators + "*******")
    if (operators(0).getClass() != c) {
      throw new IllegalStateException(s"$sqlString expected operator: $c," +
          s" but got ${operators(0)}\n physical: \n$physical")
    }
    assert(df.count() == numRows,
      "Mismatch got df.count ->" + df.count() + " but expected numRows ->" + numRows
          + " for query =" + sqlString)

    assert(df.rdd.partitions.length == numPartitions,
      "Mismatch got df.rdd.partitions.length ->" + df.rdd.partitions.length +
          " but expected numPartitions ->" + numPartitions + " for query =" + sqlString)
  }

  private def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {
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

  private def validateReplicatedTableQueries(snc: SnappyContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, 1, classOf[RowTableScan])
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, 1, classOf[RowTableScan])
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, 1, classOf[RowTableScan])
        case "Q4" => assertQuery(snc, NWQueries.Q4, 9, 1, classOf[RowTableScan])
        case "Q5" => assertQuery(snc, NWQueries.Q5, 9, 1, classOf[RowTableScan])
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, 1, classOf[RowTableScan])
        case "Q7" => assertQuery(snc, NWQueries.Q7, 9, 1, classOf[RowTableScan])
        case "Q8" => assertQuery(snc, NWQueries.Q8, 6, 1, classOf[FilterExec])
        case "Q9" => assertQuery(snc, NWQueries.Q9, 3, 1, classOf[ProjectExec])
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, 1, classOf[FilterExec])
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, 1 , classOf[ProjectExec])
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, 1 , classOf[FilterExec])
        case "Q13" => assertQuery(snc, NWQueries.Q13, 2, 1, classOf[FilterExec])
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, 1 , classOf[FilterExec])
        case "Q15" => assertQuery(snc, NWQueries.Q15, 5, 1 , classOf[FilterExec])
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, 1 , classOf[FilterExec])
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, 1 , classOf[FilterExec])
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, 1, classOf[ProjectExec])
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, 1, classOf[ProjectExec])
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, 1, classOf[ProjectExec])
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, 1, classOf[RowTableScan])
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, 1, classOf[ProjectExec])
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, 1, classOf[RowTableScan])
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, 1, classOf[ProjectExec])
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, 1, classOf[RowTableScan])
        case "Q26" => assertJoin(snc, NWQueries.Q26, 86, 1, classOf[SortMergeJoinExec])
        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, 1, classOf[SortMergeJoinExec])
        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, 1, classOf[RowTableScan])
        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, 1, classOf[SortMergeJoinExec])
        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, 1, classOf[SortMergeJoinExec])
        case "Q31" => assertJoin(snc, NWQueries.Q31, 830, 1, classOf[LocalJoin])
        case "Q32" => assertJoin(snc, NWQueries.Q32, 8, 1, classOf[LocalJoin])
        case "Q33" => assertJoin(snc, NWQueries.Q33, 37, 1, classOf[LocalJoin])
        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, 1, classOf[LocalJoin])
        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, 4, classOf[LocalJoin])
        case "Q36" => assertJoin(snc, NWQueries.Q36, 290, 1, classOf[LocalJoin])
        case "Q37" => assertJoin(snc, NWQueries.Q37, 77, 1, classOf[LocalJoin])
        //case "Q38" => assertJoin(snc, NWQueries.Q38, 2155, 1, classOf[LocalJoin]) // NPE LocalJoin
        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, 1, classOf[LocalJoin])
        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, 1, classOf[LocalJoin])
        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, 1, classOf[LocalJoin])
        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, 1, classOf[LocalJoin])
        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, 1, classOf[SortMergeJoinExec])
        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, 1, classOf[BroadcastNestedLoopJoinExec])
        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, 1,
          classOf[CartesianProductExec])
        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, 1,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, 5,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, 5,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, 5,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, 1, classOf[LocalJoin])
        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, 1, classOf[SortMergeJoinExec])
        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, 1, classOf[SortMergeJoinExec])
        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, 1, classOf[SortMergeJoinExec])
        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, 1, classOf[SortMergeJoinExec])
        case "Q55" => assertJoin(snc, NWQueries.Q55, 21, 1, classOf[LocalJoin])
        case "Q56" => assertJoin(snc, NWQueries.Q56, 8, 1, classOf[LocalJoin])
        case _ => println("ok")
      }
    }
  }

  private def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

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

    snc.sql(NWQueries.orders_table + " using row options (partition_by 'OrderId', buckets '13')")
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
        " using row options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders')")
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " using row options ( partition_by 'ProductID', buckets '17')")
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING row options (PARTITION_BY 'SupplierID', buckets '123' )")
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using row options (partition_by 'TerritoryID', buckets '3')")
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
        " using row options(partition_by 'EmployeeID', buckets '1')")
    NWQueries.employee_territories.write.insertInto("employee_territories")

  }

  private def validatePartitionedRowTableQueries(snc: SnappyContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, 1, classOf[RowTableScan])
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, 1, classOf[RowTableScan])
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, 4, classOf[RowTableScan])
        case "Q4" => assertQuery(snc, NWQueries.Q4, 9, 1, classOf[RowTableScan])
        case "Q5" => assertQuery(snc, NWQueries.Q5, 9, 1, classOf[RowTableScan])
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, 1, classOf[RowTableScan])
        case "Q7" => assertQuery(snc, NWQueries.Q7, 9, 1, classOf[RowTableScan])
        case "Q8" => assertQuery(snc, NWQueries.Q8, 6, 1, classOf[FilterExec])
        case "Q9" => assertQuery(snc, NWQueries.Q9, 3, 1, classOf[ProjectExec])
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, 1, classOf[FilterExec])
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, 1 , classOf[ProjectExec])
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, 1 , classOf[FilterExec])
        case "Q13" => assertQuery(snc, NWQueries.Q13, 2, 4, classOf[FilterExec])
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, 1 , classOf[FilterExec])
        case "Q15" => assertQuery(snc, NWQueries.Q15, 5, 1 , classOf[FilterExec])
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, 1 , classOf[FilterExec])
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, 1 , classOf[FilterExec])
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, 1, classOf[ProjectExec])
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, 4, classOf[ProjectExec])
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, 1, classOf[ProjectExec])
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, 1, classOf[RowTableScan])
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, 1, classOf[ProjectExec])
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, 1, classOf[RowTableScan])
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, 5, classOf[ProjectExec])
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, 1, classOf[RowTableScan])
//        case "Q26" => assertJoin(snc, NWQueries.Q26, 89, 1, classOf[BroadcastHashJoinExec])
//        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, 4, classOf[BroadcastHashJoinExec])
//        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, 4, classOf[RowTableScan])
//        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, 4, classOf[BroadcastHashJoinExec])
//        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, 4, classOf[BroadcastHashJoinExec])
//        case "Q31" => assertJoin(snc, NWQueries.Q31, 830, 200, classOf[LocalJoin])
//        case "Q32" => assertJoin(snc, NWQueries.Q32, 37, 29, classOf[LocalJoin])
//        case "Q33" => assertJoin(snc, NWQueries.Q33, 37, 10, classOf[LocalJoin])
//        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, 200, classOf[BroadcastHashJoinExec])
//        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, 4, classOf[LocalJoin])
//        case "Q36" => assertJoin(snc, NWQueries.Q36, 292, 165, classOf[BroadcastHashJoinExec])
//        case "Q37" => assertJoin(snc, NWQueries.Q37, 0, 1, classOf[BroadcastHashJoinExec]) // 77
//        case "Q38" => assertJoin(snc, NWQueries.Q38, 2155, 200, classOf[SortMergeJoinExec]) // NPE LocalJoin
//        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, 4, classOf[LocalJoin])
//        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, 4, classOf[LocalJoin])
//        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, 4, classOf[LocalJoin])
//        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, 200, classOf[LocalJoin])
//        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, 4, classOf[SortMergeJoinExec])
//        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, 4, classOf[BroadcastNestedLoopJoinExec])
//        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, 8,
//          classOf[CartesianProductExec])
//        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, 4, classOf[LocalJoin])
//        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, 4, classOf[SortMergeJoinExec])
//        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, 4, classOf[SortMergeJoinExec])
//        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, 4, classOf[SortMergeJoinExec])
//        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, 4, classOf[SortMergeJoinExec])
//        case "Q55" => assertJoin(snc, NWQueries.Q55, 21, 1, classOf[LocalJoin])
//        case "Q56" => assertJoin(snc, NWQueries.Q56, 8, 1, classOf[LocalJoin])
        case _ => println("ok")
      }
    }
  }

  private def createAndLoadColumnTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table + " using column options()")
    NWQueries.employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    NWQueries.customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using column options (partition_by 'OrderId', buckets '13')")
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
        " using column options (partition_by 'OrderId', buckets '13', COLOCATE_WITH 'orders')")
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " USING column options ( partition_by 'ProductID,SupplierID', buckets '17')")
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '123' )")
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using column options (partition_by 'TerritoryID', buckets '3')")
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
        " using row options(partition_by 'EmployeeID', buckets '1')")
    NWQueries.employee_territories.write.insertInto("employee_territories")
  }

  private def validatePartitionedColumnTableQueries(snc: SnappyContext): Unit = {

    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, 1, classOf[RowTableScan])
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, 1, classOf[RowTableScan])
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, 4, classOf[ColumnTableScan])
        case "Q4" => assertQuery(snc, NWQueries.Q4, 9, 4, classOf[ColumnTableScan])
        case "Q5" => assertQuery(snc, NWQueries.Q5, 9, 10, classOf[ColumnTableScan])
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, 10, classOf[ColumnTableScan])
        case "Q7" => assertQuery(snc, NWQueries.Q7, 9, 10, classOf[ColumnTableScan])
        case "Q8" => assertQuery(snc, NWQueries.Q8, 6, 4, classOf[FilterExec])
        case "Q9" => assertQuery(snc, NWQueries.Q9, 3, 4, classOf[ProjectExec])
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, 4, classOf[FilterExec])
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, 4, classOf[ProjectExec])
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, 3, classOf[FilterExec])
        case "Q13" => assertQuery(snc, NWQueries.Q13, 2, 4, classOf[FilterExec])
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, 1, classOf[FilterExec])
        case "Q15" => assertQuery(snc, NWQueries.Q15, 5, 4, classOf[FilterExec])
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, 4, classOf[FilterExec])
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, 4, classOf[FilterExec])
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, 4, classOf[ProjectExec])
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, 4, classOf[ProjectExec])
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, 1, classOf[ProjectExec])
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, 1, classOf[ColumnTableScan])
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, 2, classOf[ProjectExec])
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, 1, classOf[ColumnTableScan])
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, 5, classOf[ProjectExec])
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, 1, classOf[RowTableScan])
//        case "Q26" => assertJoin(snc, NWQueries.Q26, 89, 200, classOf[SortMergeJoinExec])
//        case "Q27" => assertJoin(snc, NWQueries.Q27, 9, 4, classOf[BroadcastHashJoinExec])
//        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, 200, classOf[ColumnTableScan])
//        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, 200, classOf[SortMergeJoinExec])
//        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, 200, classOf[SortMergeJoinExec])
//        case "Q31" => assertJoin(snc, NWQueries.Q31, 830, 200, classOf[LocalJoin])
//        case "Q32" => assertJoin(snc, NWQueries.Q32, 37, 1, classOf[LocalJoin])
//        case "Q33" => assertJoin(snc, NWQueries.Q33, 37, 1, classOf[LocalJoin])
//        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, 1, classOf[LocalJoin])
//        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, 4, classOf[LocalJoin])
//        case "Q36" => assertJoin(snc, NWQueries.Q36, 292, 1, classOf[LocalJoin])
//        case "Q37" => assertJoin(snc, NWQueries.Q37, 0, 1, classOf[LocalJoin]) // 77
//        case "Q38" => assertJoin(snc, NWQueries.Q38, 2155, 1, classOf[LocalJoin]) NPE LocalJoin
//        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, 1, classOf[LocalJoin])
//        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, 1, classOf[LocalJoin])
//        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, 1, classOf[LocalJoin])
//        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, 1, classOf[LocalJoin])
//        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, 1, classOf[SortMergeJoinExec])
//        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, 1, classOf[BroadcastNestedLoopJoinExec])
//        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, 1,
//          classOf[CartesianProductExec])
//        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, 1,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, 5,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, 5,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, 5,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, 1, classOf[LocalJoin])
//        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, 1, classOf[SortMergeJoinExec])
//        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, 1, classOf[SortMergeJoinExec])
//        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, 1, classOf[SortMergeJoinExec])
//        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, 1, classOf[SortMergeJoinExec])
//        case "Q55" => assertJoin(snc, NWQueries.Q55, 21, 1, classOf[LocalJoin])
//        case "Q56" => assertJoin(snc, NWQueries.Q56, 8, 1, classOf[LocalJoin])
        case _ => println("ok")
      }
    }
  }

  private def createAndLoadColocatedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions.write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories.write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers.write.insertInto("shippers")

    snc.sql(NWQueries.employees_table +
        " using row options( partition_by 'EmployeeID', buckets '3')")
    NWQueries.employees.write.insertInto("employees")

    snc.sql(NWQueries.customers_table +
        " using column options( partition_by 'CustomerID', buckets '19')")
    NWQueries.customers.write.insertInto("customers")

    snc.sql(NWQueries.orders_table +
        " using row options (partition_by 'CustomerID', buckets '19', colocate_with 'customers')")
    NWQueries.orders.write.insertInto("orders")

    snc.sql(NWQueries.order_details_table +
        " using row options ( partition_by 'ProductID', buckets '329')")
    NWQueries.order_details.write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " USING column options ( partition_by 'ProductID', buckets '329'," +
        " colocate_with 'order_details')")
    NWQueries.products.write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '123')")
    NWQueries.suppliers.write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using column options (partition_by 'TerritoryID', buckets '3')")
    NWQueries.territories.write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
        " using row options(partition_by 'TerritoryID', buckets '3', colocate_with 'territories') ")
    NWQueries.employee_territories.write.insertInto("employee_territories")

  }


  private def validateColocatedTableQueries(snc: SnappyContext): Unit = {

    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQuery(snc, NWQueries.Q1, 8, 1, classOf[RowTableScan])
        case "Q2" => assertQuery(snc, NWQueries.Q2, 91, 4, classOf[ColumnTableScan])
        case "Q3" => assertQuery(snc, NWQueries.Q3, 830, 4, classOf[RowTableScan])
        case "Q4" => assertQuery(snc, NWQueries.Q4, 9, 3, classOf[RowTableScan])
        case "Q5" => assertQuery(snc, NWQueries.Q5, 9, 10, classOf[RowTableScan])
        case "Q6" => assertQuery(snc, NWQueries.Q6, 9, 10, classOf[RowTableScan])
        case "Q7" => assertQuery(snc, NWQueries.Q7, 9, 10, classOf[RowTableScan])
        case "Q8" => assertQuery(snc, NWQueries.Q8, 6, 3, classOf[FilterExec])
        case "Q9" => assertQuery(snc, NWQueries.Q9, 3, 3, classOf[ProjectExec])
        case "Q10" => assertQuery(snc, NWQueries.Q10, 2, 3, classOf[FilterExec])
        case "Q11" => assertQuery(snc, NWQueries.Q11, 0, 3, classOf[ProjectExec])
        case "Q12" => assertQuery(snc, NWQueries.Q12, 2, 3, classOf[FilterExec])
        case "Q13" => assertQuery(snc, NWQueries.Q13, 2, 4, classOf[FilterExec])
        case "Q14" => assertQuery(snc, NWQueries.Q14, 91, 92, classOf[FilterExec])
        case "Q15" => assertQuery(snc, NWQueries.Q15, 5, 3, classOf[FilterExec])
        case "Q16" => assertQuery(snc, NWQueries.Q16, 8, 3, classOf[FilterExec])
        case "Q17" => assertQuery(snc, NWQueries.Q17, 3, 3, classOf[FilterExec])
        case "Q18" => assertQuery(snc, NWQueries.Q18, 9, 3, classOf[ProjectExec])
        case "Q19" => assertQuery(snc, NWQueries.Q19, 13, 4, classOf[ProjectExec])
        case "Q20" => assertQuery(snc, NWQueries.Q20, 1, 1, classOf[ProjectExec])
        case "Q21" => assertQuery(snc, NWQueries.Q21, 1, 1, classOf[RowTableScan])
        case "Q22" => assertQuery(snc, NWQueries.Q22, 1, 2, classOf[ProjectExec])
        case "Q23" => assertQuery(snc, NWQueries.Q23, 1, 1, classOf[RowTableScan])
        case "Q24" => assertQuery(snc, NWQueries.Q24, 4, 5, classOf[ProjectExec])
        case "Q25" => assertJoin(snc, NWQueries.Q25, 1, 4, classOf[ColumnTableScan])
        case "Q26" => assertJoin(snc, NWQueries.Q26, 86, 4, classOf[BroadcastHashJoinExec])
//       // case "Q27" => assertJoin(snc, NWQueries.Q27, 9, 4, classOf[SortMergeJoinExec])
//        case "Q28" => assertJoin(snc, NWQueries.Q28, 12, 4, classOf[ColumnTableScan])
//        case "Q29" => assertJoin(snc, NWQueries.Q29, 8, 4, classOf[BroadcastHashJoinExec])
//        case "Q30" => assertJoin(snc, NWQueries.Q30, 8, 4, classOf[BroadcastHashJoinExec])
//        case "Q31" => assertJoin(snc, NWQueries.Q31, 830, 200, classOf[BroadcastHashJoinExec])
//        case "Q32" => assertJoin(snc, NWQueries.Q32, 37, 29, classOf[BroadcastHashJoinExec])
//        case "Q33" => assertJoin(snc, NWQueries.Q33, 37, 10, classOf[BroadcastHashJoinExec])
//        case "Q34" => assertJoin(snc, NWQueries.Q34, 5, 200, classOf[BroadcastHashJoinExec])
//        case "Q35" => assertJoin(snc, NWQueries.Q35, 3, 4, classOf[BroadcastHashJoinExec])
//        case "Q36" => assertJoin(snc, NWQueries.Q36, 292, 165, classOf[BroadcastHashJoinExec])
//        case "Q37" => assertJoin(snc, NWQueries.Q37, 0, 4, classOf[BroadcastHashJoinExec]) // 77
//        //        case "Q38" => assertJoin(snc, NWQueries.Q38, 2155, 1, classOf[LocalJoin]) NPE LocalJoin
//        case "Q39" => assertJoin(snc, NWQueries.Q39, 9, 4, classOf[BroadcastHashJoinExec])
//        case "Q40" => assertJoin(snc, NWQueries.Q40, 830, 4, classOf[BroadcastHashJoinExec])
//        case "Q41" => assertJoin(snc, NWQueries.Q41, 2155, 4, classOf[BroadcastHashJoinExec])
//        case "Q42" => assertJoin(snc, NWQueries.Q42, 22, 4, classOf[BroadcastHashJoinExec])
//        case "Q43" => assertJoin(snc, NWQueries.Q43, 830, 4, classOf[SortMergeJoinExec])
//        case "Q44" => assertJoin(snc, NWQueries.Q44, 830, 4, classOf[BroadcastNestedLoopJoinExec])
//        case "Q45" => assertJoin(snc, NWQueries.Q45, 1788650, 8,
//          classOf[CartesianProductExec])
//        case "Q46" => assertJoin(snc, NWQueries.Q46, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q47" => assertJoin(snc, NWQueries.Q47, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q48" => assertJoin(snc, NWQueries.Q48, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q49" => assertJoin(snc, NWQueries.Q49, 1788650, 8,
//          classOf[BroadcastNestedLoopJoinExec])
//        case "Q50" => assertJoin(snc, NWQueries.Q50, 2155, 4, classOf[LocalJoin])
//        case "Q51" => assertJoin(snc, NWQueries.Q51, 2155, 4, classOf[SortMergeJoinExec])
//        case "Q52" => assertJoin(snc, NWQueries.Q52, 2155, 4, classOf[SortMergeJoinExec])
//        case "Q53" => assertJoin(snc, NWQueries.Q53, 2155, 200, classOf[SortMergeJoinExec])
//        case "Q54" => assertJoin(snc, NWQueries.Q54, 2155, 200, classOf[SortMergeJoinExec])
//        case "Q55" => assertJoin(snc, NWQueries.Q55, 21, 1, classOf[LocalJoin])
//        case "Q56" => assertJoin(snc, NWQueries.Q56, 8, 1, classOf[LocalJoin])
        case _ => println("ok")
      }
    }
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
