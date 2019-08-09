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
package org.apache.spark.sql

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{ResultSet, Statement}

import scala.io.Source

import io.snappydata.cluster.{ClusterManagerTestBase, DisableSparkTestingFlag}
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}

class NorthWindDUnitTest(s: String) extends ClusterManagerTestBase(s)
  with DisableSparkTestingFlag {

  override val locatorNetPort: Int = AvailablePortHelper.getRandomAvailableTCPPort
  protected val productDir: String = SmartConnectorFunctions.getEnvironmentVariable("SNAPPY_HOME")
  override val stopNetServersInTearDown = false


  override def beforeClass(): Unit = {
    super.beforeClass()
    startNetworkServersOnAllVMs()
    vm3.invoke(classOf[ClusterManagerTestBase], "startSparkCluster", productDir)
  }

  override def afterClass(): Unit = {
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
    ClusterManagerTestBase.stopNetworkServers()
    super.afterClass()
    Array(vm0, vm1, vm2).foreach(_.invoke(classOf[ClusterManagerTestBase],
      "validateNoActiveSnapshotTX"))
    vm3.invoke(classOf[ClusterManagerTestBase], "stopSparkCluster", productDir)
  }

  def testReplicatedTableQueries(): Unit = {
    val snc = SnappyContext(sc)
    val sqlContext = new SparkSession(sc).sqlContext
    val pw = new PrintWriter(new FileOutputStream(
      new File("ValidateNWQueries_ReplicatedTable.out"), true))
    try {
      NorthWindDUnitTest.createAndLoadReplicatedTables(snc)
      NorthWindDUnitTest.createAndLoadSparkTables(sqlContext)
      // validateReplicatedTableQueries(snc)
      NorthWindDUnitTest.validateQueriesFullResultSet(snc, "ReplicatedTable", pw, sqlContext)
    } finally {
      pw.close()
    }
  }

  def testPartitionedRowTableQueries(): Unit = {
    val snc = SnappyContext(sc)
    val sqlContext = new SparkSession(sc).sqlContext
    val pw = new PrintWriter(new FileOutputStream(
      new File("ValidateNWQueries_PartitionedRowTable.out"), true))
    try {
      createAndLoadPartitionedTables(snc)
      NorthWindDUnitTest.createAndLoadSparkTables(sqlContext)
      // validatePartitionedRowTableQueries(snc)
      NorthWindDUnitTest.validateQueriesFullResultSet(snc, "PartitionedRowTable", pw, sqlContext)
    } finally {
      pw.close()
    }
  }

  def testPartitionedColumnTableQueries(): Unit = {
    val snc = SnappyContext(sc)
    val sqlContext = new SparkSession(sc).sqlContext
    val pw = new PrintWriter(new FileOutputStream(
      new File("ValidateNWQueries_ColumnTable.out"), true))
    try {
      NorthWindDUnitTest.createAndLoadColumnTables(snc)
      NorthWindDUnitTest.createAndLoadSparkTables(sqlContext)
      // validatePartitionedColumnTableQueries(snc)
      NorthWindDUnitTest.validateQueriesFullResultSet(snc, "ColumnTable", pw, sqlContext)

      // verify the colocated table queries in smart connector mode
      val params = Array(locatorNetPort, "ColumnTable").
          asInstanceOf[Array[AnyRef]]
      vm3.invoke(classOf[SmartConnectorFunctions], "nwQueryValidationOnConnector", params)
    } finally {
      pw.close()
    }
  }

  def testColocatedTableQueries(): Unit = {
    val snc = SnappyContext(sc)
    val sqlContext = new SparkSession(sc).sqlContext
    val pw = new PrintWriter(new FileOutputStream(
      new File("ValidateNWQueries_ColocatedTable.out"), true))
    try {
      NorthWindDUnitTest.createAndLoadColocatedTables(snc)
      NorthWindDUnitTest.createAndLoadSparkTables(sqlContext)
      // validateColocatedTableQueries(snc)

      NorthWindDUnitTest.validateQueriesFullResultSet(snc, "ColocatedTable", pw, sqlContext)

      // verify the colocated table queries in smart connector mode
      val params = Array(locatorNetPort, "ColocatedTable").
          asInstanceOf[Array[AnyRef]]
      vm3.invoke(classOf[SmartConnectorFunctions], "nwQueryValidationOnConnector", params)
    } finally {
      pw.close()
    }
  }

  def testInsertionOfRecordInColumnTable(): Unit = {
    val snc = SnappyContext(sc)
    val netPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort)
    val conn = getANetConnection(netPort)

    val s = conn.createStatement()
    createAndLoadColumnTableUsingJDBC(s, snc)
    val rs: ResultSet = s.executeQuery(s"SELECT * from products")
    assert(rs.next())
    conn.close()
  }

  private lazy val totalProcessors = Utils.mapExecutors[Int](sc, () =>
    Iterator(Runtime.getRuntime.availableProcessors())).sum

  private def validateReplicatedTableQueries(snc: SnappyContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => NWQueries.assertQuery(snc, NWQueries.Q1, "Q1", 8, 1, classOf[RowTableScan])
        case "Q2" => NWQueries.assertQuery(snc, NWQueries.Q2, "Q2", 91, 1, classOf[RowTableScan])
        case "Q3" => NWQueries.assertQuery(snc, NWQueries.Q3, "Q3", 830, 1, classOf[RowTableScan])
        case "Q4" => NWQueries.assertQuery(snc, NWQueries.Q4, "Q4", 9, 1, classOf[RowTableScan])
        case "Q5" => NWQueries.assertQuery(snc, NWQueries.Q5, "Q5", 9, 1, classOf[RowTableScan])
        case "Q6" => NWQueries.assertQuery(snc, NWQueries.Q6, "Q6", 9, 1, classOf[RowTableScan])
        case "Q7" => NWQueries.assertQuery(snc, NWQueries.Q7, "Q7", 9, 1, classOf[RowTableScan])
        case "Q8" => NWQueries.assertQuery(snc, NWQueries.Q8, "Q8", 6, 1, classOf[FilterExec])
        case "Q9" => NWQueries.assertQuery(snc, NWQueries.Q9, "Q9", 3, 1, classOf[ProjectExec])
        case "Q10" => NWQueries.assertQuery(snc, NWQueries.Q10, "Q10", 2, 1, classOf[FilterExec])
        case "Q11" => NWQueries.assertQuery(snc, NWQueries.Q11, "Q11", 4, 1, classOf[ProjectExec])
        case "Q12" => NWQueries.assertQuery(snc, NWQueries.Q12, "Q12", 2, 1, classOf[FilterExec])
        case "Q13" => NWQueries.assertQuery(snc, NWQueries.Q13, "Q13", 2, 1, classOf[FilterExec])
        case "Q14" => NWQueries.assertQuery(snc, NWQueries.Q14, "Q14", 69, 1, classOf[FilterExec])
        case "Q15" => NWQueries.assertQuery(snc, NWQueries.Q15, "Q15", 5, 1, classOf[FilterExec])
        case "Q16" => NWQueries.assertQuery(snc, NWQueries.Q16, "Q16", 8, 1, classOf[FilterExec])
        case "Q17" => NWQueries.assertQuery(snc, NWQueries.Q17, "Q17", 3, 1, classOf[FilterExec])
        case "Q18" => NWQueries.assertQuery(snc, NWQueries.Q18, "Q18", 9, 1, classOf[ProjectExec])
        case "Q19" => NWQueries.assertQuery(snc, NWQueries.Q19, "Q19", 13, 1, classOf[ProjectExec])
        case "Q20" => NWQueries.assertQuery(snc, NWQueries.Q20, "Q20", 1, 1, classOf[ProjectExec])
        case "Q21" => NWQueries.assertQuery(snc, NWQueries.Q21, "Q21", 1, 1, classOf[RowTableScan])
        case "Q22" => NWQueries.assertQuery(snc, NWQueries.Q22, "Q22", 1, 1, classOf[ProjectExec])
        case "Q23" => NWQueries.assertQuery(snc, NWQueries.Q23, "Q23", 1, 1, classOf[RowTableScan])
        case "Q24" => NWQueries.assertQuery(snc, NWQueries.Q24, "Q24", 4, 1, classOf[ProjectExec])
        case "Q25" => NWQueries.assertJoin(snc, NWQueries.Q25, "Q25", 1, 1, classOf[RowTableScan])
        case "Q26" => NWQueries.assertJoin(snc, NWQueries.Q26, "Q26", 86, 1,
          classOf[SortMergeJoinExec])
        case "Q27" => NWQueries.assertJoin(snc, NWQueries.Q27, "Q27", 9, 1,
          classOf[SortMergeJoinExec])
        case "Q28" => NWQueries.assertJoin(snc, NWQueries.Q28, "Q28", 12, 1, classOf[RowTableScan])
        case "Q29" => NWQueries.assertJoin(snc, NWQueries.Q29, "Q29", 8, 1,
          classOf[SortMergeJoinExec])
        case "Q30" => NWQueries.assertJoin(snc, NWQueries.Q30, "Q30", 8, 1,
          classOf[SortMergeJoinExec])
        case "Q31" => NWQueries.assertJoin(snc, NWQueries.Q31, "Q31", 830, 1, classOf[HashJoinExec])
        case "Q32" => NWQueries.assertJoin(snc, NWQueries.Q32, "Q32", 8, 1, classOf[HashJoinExec])
        case "Q33" => NWQueries.assertJoin(snc, NWQueries.Q33, "Q33", 37, 1, classOf[HashJoinExec])
        case "Q34" => NWQueries.assertJoin(snc, NWQueries.Q34, "Q34", 5, 1, classOf[HashJoinExec])
        case "Q35" => NWQueries.assertJoin(snc, NWQueries.Q35, "Q35", 3, 4, classOf[HashJoinExec])
        case "Q36" => NWQueries.assertJoin(snc, NWQueries.Q36, "Q36", 290, 1, classOf[HashJoinExec])
        case "Q37" => NWQueries.assertJoin(snc, NWQueries.Q37, "Q37", 77, totalProcessors,
          classOf[HashJoinExec])
        case "Q38" => NWQueries.assertJoin(snc, NWQueries.Q38, "Q38", 2155, 1,
          classOf[HashJoinExec])
        case "Q39" => NWQueries.assertJoin(snc, NWQueries.Q39, "Q39", 9, 1, classOf[HashJoinExec])
        case "Q40" => NWQueries.assertJoin(snc, NWQueries.Q40, "Q40", 830, 1, classOf[HashJoinExec])
        case "Q41" => NWQueries.assertJoin(snc, NWQueries.Q41, "Q41", 2155, 1,
          classOf[HashJoinExec])
        case "Q42" => NWQueries.assertJoin(snc, NWQueries.Q42, "Q42", 22, 1, classOf[HashJoinExec])
        case "Q43" => NWQueries.assertJoin(snc, NWQueries.Q43, "Q43", 830, 1,
          classOf[SortMergeJoinExec])
        case "Q44" => NWQueries.assertJoin(snc, NWQueries.Q44, "Q44", 830, 1,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q45" => NWQueries.assertJoin(snc, NWQueries.Q45, "Q45", 1788650, 1,
          classOf[CartesianProductExec])
        case "Q46" => NWQueries.assertJoin(snc, NWQueries.Q46, "Q46", 1788650, 1,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q47" => NWQueries.assertJoin(snc, NWQueries.Q47, "Q47", 1788650, 1,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q48" => NWQueries.assertJoin(snc, NWQueries.Q48, "Q48", 1788650,
          totalProcessors * 2 + 1, classOf[BroadcastNestedLoopJoinExec])
        case "Q49" => NWQueries.assertJoin(snc, NWQueries.Q49, "Q49", 1788650,
          totalProcessors * 2 + 1, classOf[BroadcastNestedLoopJoinExec])
        case "Q50" => NWQueries.assertJoin(snc, NWQueries.Q50, "Q50", 2155, 1,
          classOf[HashJoinExec])
        case "Q51" => NWQueries.assertJoin(snc, NWQueries.Q51, "Q51", 2155, 1,
          classOf[SortMergeJoinExec])
        case "Q52" => NWQueries.assertJoin(snc, NWQueries.Q52, "Q52", 2155, 1,
          classOf[SortMergeJoinExec])
        case "Q53" => NWQueries.assertJoin(snc, NWQueries.Q53, "Q53", 2155, 1,
          classOf[SortMergeJoinExec])
        case "Q54" => NWQueries.assertJoin(snc, NWQueries.Q54, "Q54", 2155, 1,
          classOf[SortMergeJoinExec])
        case "Q55" => NWQueries.assertJoin(snc, NWQueries.Q55, "Q55", 21, 1, classOf[HashJoinExec])
        case "Q56" => NWQueries.assertJoin(snc, NWQueries.Q56, "Q56", 8, 1, classOf[HashJoinExec])
      }
    }
  }

  private def createAndLoadPartitionedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers(snc).write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using row options (" +
        "partition_by 'OrderId', buckets '8', redundancy '1')")
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table + " using row options (" +
        "partition_by 'OrderId', buckets '8', COLOCATE_WITH 'orders', " +
        "redundancy '1')")
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " using row options ( partition_by 'ProductID', buckets '16')")
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING row options (PARTITION_BY 'SupplierID', buckets '12' )")
    NWQueries.suppliers(snc).write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using row options (partition_by 'TerritoryID', buckets '4')")
    NWQueries.territories(snc).write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
        " using row options(partition_by 'EmployeeID', buckets '1')")
    NWQueries.employee_territories(snc).write.insertInto("employee_territories")

  }

  private def validatePartitionedRowTableQueries(snc: SnappyContext): Unit = {
    val numDefaultPartitions = ((totalProcessors - 4) to (totalProcessors + 4)).toArray
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => NWQueries.assertQuery(snc, NWQueries.Q1, "Q1", 8, 1, classOf[RowTableScan])
        case "Q2" => NWQueries.assertQuery(snc, NWQueries.Q2, "Q2", 91, 1, classOf[RowTableScan])
        case "Q3" => NWQueries.assertQuery(snc, NWQueries.Q3, "Q3", 830, numDefaultPartitions,
          classOf[RowTableScan])
        case "Q4" => NWQueries.assertQuery(snc, NWQueries.Q4, "Q4", 9, 1, classOf[RowTableScan])
        case "Q5" => NWQueries.assertQuery(snc, NWQueries.Q5, "Q5", 9, 1, classOf[RowTableScan])
        case "Q6" => NWQueries.assertQuery(snc, NWQueries.Q6, "Q6", 9, 1, classOf[RowTableScan])
        case "Q7" => NWQueries.assertQuery(snc, NWQueries.Q7, "Q7", 9, 1, classOf[RowTableScan])
        case "Q8" => NWQueries.assertQuery(snc, NWQueries.Q8, "Q8", 6, 1, classOf[FilterExec])
        case "Q9" => NWQueries.assertQuery(snc, NWQueries.Q9, "Q9", 3, 1, classOf[ProjectExec])
        case "Q10" => NWQueries.assertQuery(snc, NWQueries.Q10, "Q10", 2, 1, classOf[FilterExec])
        case "Q11" => NWQueries.assertQuery(snc, NWQueries.Q11, "Q11", 4, 1, classOf[ProjectExec])
        case "Q12" => NWQueries.assertQuery(snc, NWQueries.Q12, "Q12", 2, 1, classOf[FilterExec])
        case "Q13" => NWQueries.assertQuery(snc, NWQueries.Q13, "Q13", 2, numDefaultPartitions,
          classOf[FilterExec])
        case "Q14" => NWQueries.assertQuery(snc, NWQueries.Q14, "Q14", 69, 1, classOf[FilterExec])
        case "Q15" => NWQueries.assertQuery(snc, NWQueries.Q15, "Q15", 5, 1, classOf[FilterExec])
        case "Q16" => NWQueries.assertQuery(snc, NWQueries.Q16, "Q16", 8, 1, classOf[FilterExec])
        case "Q17" => NWQueries.assertQuery(snc, NWQueries.Q17, "Q17", 3, 1, classOf[FilterExec])
        case "Q18" => NWQueries.assertQuery(snc, NWQueries.Q18, "Q18", 9, 1, classOf[ProjectExec])
        case "Q19" => NWQueries.assertQuery(snc, NWQueries.Q19, "Q19", 13, numDefaultPartitions,
          classOf[ProjectExec])
        case "Q20" => NWQueries.assertQuery(snc, NWQueries.Q20, "Q20", 1, 1, classOf[ProjectExec])
        case "Q21" => NWQueries.assertQuery(snc, NWQueries.Q21, "Q21", 1, 1, classOf[RowTableScan])
        case "Q22" => NWQueries.assertQuery(snc, NWQueries.Q22, "Q22", 1, 1, classOf[ProjectExec])
        case "Q23" => NWQueries.assertQuery(snc, NWQueries.Q23, "Q23", 1, 1, classOf[RowTableScan])
        case "Q24" => NWQueries.assertQuery(snc, NWQueries.Q24, "Q24", 4, 4, classOf[ProjectExec])
        case "Q25" => NWQueries.assertJoin(snc, NWQueries.Q25, "Q25", 1, 1, classOf[RowTableScan])
        case "Q26" => NWQueries.assertJoin(snc, NWQueries.Q26, "Q26", 86, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q27" => NWQueries.assertJoin(snc, NWQueries.Q27, "Q27", 9, 12,
          classOf[BroadcastHashJoinExec])
        case "Q28" => NWQueries.assertJoin(snc, NWQueries.Q28, "Q28", 12, totalProcessors,
          classOf[RowTableScan])
        case "Q29" => NWQueries.assertJoin(snc, NWQueries.Q29, "Q29", 8, 12,
          classOf[BroadcastHashJoinExec])
        case "Q30" => NWQueries.assertJoin(snc, NWQueries.Q30, "Q30", 8, 12,
          classOf[BroadcastHashJoinExec])
        case "Q31" => NWQueries.assertJoin(snc, NWQueries.Q31, "Q31", 830, totalProcessors,
          classOf[HashJoinExec])
        case "Q32" => NWQueries.assertJoin(snc, NWQueries.Q32, "Q32", 8, 8, classOf[HashJoinExec])
        case "Q33" => NWQueries.assertJoin(snc, NWQueries.Q33, "Q33", 37, 8, classOf[HashJoinExec])
        case "Q34" => NWQueries.assertJoin(snc, NWQueries.Q34, "Q34", 5, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q35" => NWQueries.assertJoin(snc, NWQueries.Q35, "Q35", 3, 4, classOf[HashJoinExec])
        case "Q36" => NWQueries.assertJoin(snc, NWQueries.Q36, "Q36", 290, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q37" => NWQueries.assertJoin(snc, NWQueries.Q37, "Q37", 77, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q38" => NWQueries.assertJoin(snc, NWQueries.Q38, "Q38", 2155, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q39" => NWQueries.assertJoin(snc, NWQueries.Q39, "Q39", 9, 12,
          classOf[HashJoinExec])
        case "Q40" => NWQueries.assertJoin(snc, NWQueries.Q40, "Q40", 830, totalProcessors,
          classOf[HashJoinExec])
        case "Q41" => NWQueries.assertJoin(snc, NWQueries.Q41, "Q41", 2155, 8,
          classOf[HashJoinExec])
        case "Q42" => NWQueries.assertJoin(snc, NWQueries.Q42, "Q42", 22, totalProcessors,
          classOf[HashJoinExec])
        case "Q43" => NWQueries.assertJoin(snc, NWQueries.Q43, "Q43", 830, 8,
          classOf[SortMergeJoinExec])
        case "Q44" => NWQueries.assertJoin(snc, NWQueries.Q44, "Q44", 830, 8,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q45" => NWQueries.assertJoin(snc, NWQueries.Q45, "Q45", 1788650, 8,
          classOf[CartesianProductExec])
        case "Q46" => NWQueries.assertJoin(snc, NWQueries.Q46, "Q46", 1788650, 8,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q47" => NWQueries.assertJoin(snc, NWQueries.Q47, "Q47", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q48" => NWQueries.assertJoin(snc, NWQueries.Q48, "Q48", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q49" => NWQueries.assertJoin(snc, NWQueries.Q49, "Q49", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q50" => NWQueries.assertJoin(snc, NWQueries.Q50, "Q50", 2155, 8,
          classOf[HashJoinExec])
        case "Q51" => NWQueries.assertJoin(snc, NWQueries.Q51, "Q51", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q52" => NWQueries.assertJoin(snc, NWQueries.Q52, "Q52", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q53" => NWQueries.assertJoin(snc, NWQueries.Q53, "Q53", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q54" => NWQueries.assertJoin(snc, NWQueries.Q54, "Q54", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q55" => NWQueries.assertJoin(snc, NWQueries.Q55, "Q55", 21, totalProcessors,
          classOf[HashJoinExec])
        case "Q56" => NWQueries.assertJoin(snc, NWQueries.Q56, "Q56", 8, 1, classOf[HashJoinExec])
      }
    }
  }

  def validatePartitionedColumnTableQueries(snc: SnappyContext): Unit = {
    val numDefaultPartitions = ((totalProcessors - 4) to (totalProcessors + 4)).toArray
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => NWQueries.assertQuery(snc, NWQueries.Q1, "Q1", 8, 1, classOf[RowTableScan])
        case "Q2" => NWQueries.assertQuery(snc, NWQueries.Q2, "Q2", 91, 1, classOf[RowTableScan])
        case "Q3" => NWQueries.assertQuery(snc, NWQueries.Q3, "Q3", 830, numDefaultPartitions,
          classOf[ColumnTableScan])
        case "Q4" => NWQueries.assertQuery(snc, NWQueries.Q4, "Q4", 9, totalProcessors,
          classOf[ColumnTableScan])
        case "Q5" => NWQueries.assertQuery(snc, NWQueries.Q5, "Q5", 9, 10, classOf[ColumnTableScan])
        case "Q6" => NWQueries.assertQuery(snc, NWQueries.Q6, "Q6", 9, 10, classOf[ColumnTableScan])
        case "Q7" => NWQueries.assertQuery(snc, NWQueries.Q7, "Q7", 9, 10, classOf[ColumnTableScan])
        case "Q8" => NWQueries.assertQuery(snc, NWQueries.Q8, "Q8", 6, totalProcessors,
          classOf[FilterExec])
        case "Q9" => NWQueries.assertQuery(snc, NWQueries.Q9, "Q9", 3, totalProcessors,
          classOf[ProjectExec])
        case "Q10" => NWQueries.assertQuery(snc, NWQueries.Q10, "Q10", 2, totalProcessors,
          classOf[FilterExec])
        case "Q11" => NWQueries.assertQuery(snc, NWQueries.Q11, "Q11", 4, totalProcessors,
          classOf[ProjectExec])
        case "Q12" => NWQueries.assertQuery(snc, NWQueries.Q12, "Q12", 2, 4, classOf[FilterExec])
        case "Q13" => NWQueries.assertQuery(snc, NWQueries.Q13, "Q13", 2, numDefaultPartitions,
          classOf[FilterExec])
        case "Q14" => NWQueries.assertQuery(snc, NWQueries.Q14, "Q14", 69, 1, classOf[FilterExec])
        case "Q15" => NWQueries.assertQuery(snc, NWQueries.Q15, "Q15", 5, totalProcessors,
          classOf[FilterExec])
        case "Q16" => NWQueries.assertQuery(snc, NWQueries.Q16, "Q16", 8, totalProcessors,
          classOf[FilterExec])
        case "Q17" => NWQueries.assertQuery(snc, NWQueries.Q17, "Q17", 3, totalProcessors,
          classOf[FilterExec])
        case "Q18" => NWQueries.assertQuery(snc, NWQueries.Q18, "Q18", 9, totalProcessors,
          classOf[ProjectExec])
        case "Q19" => NWQueries.assertQuery(snc, NWQueries.Q19, "Q19", 13, numDefaultPartitions,
          classOf[ProjectExec])
        case "Q20" => NWQueries.assertQuery(snc, NWQueries.Q20, "Q20", 1, 1, classOf[ProjectExec])
        case "Q21" => NWQueries.assertQuery(snc, NWQueries.Q21, "Q21", 1, 1,
          classOf[ColumnTableScan])
        case "Q22" => NWQueries.assertQuery(snc, NWQueries.Q22, "Q22", 1, 2, classOf[ProjectExec])
        case "Q23" => NWQueries.assertQuery(snc, NWQueries.Q23, "Q23", 1, 1,
          classOf[ColumnTableScan])
        case "Q24" => NWQueries.assertQuery(snc, NWQueries.Q24, "Q24", 4, 4, classOf[ProjectExec])
        case "Q25" => NWQueries.assertJoin(snc, NWQueries.Q25, "Q25", 1, 1, classOf[RowTableScan])
        case "Q26" => NWQueries.assertJoin(snc, NWQueries.Q26, "Q26", 86, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q27" => NWQueries.assertJoin(snc, NWQueries.Q27, "Q27", 9, 12,
          classOf[BroadcastHashJoinExec])
        case "Q28" => NWQueries.assertJoin(snc, NWQueries.Q28, "Q28", 12, totalProcessors,
          classOf[ColumnTableScan])
        case "Q29" => NWQueries.assertJoin(snc, NWQueries.Q29, "Q29", 8, 12,
          classOf[SortMergeJoinExec])
        case "Q30" => NWQueries.assertJoin(snc, NWQueries.Q30, "Q30", 8, 12,
          classOf[SortMergeJoinExec])
        case "Q31" => NWQueries.assertJoin(snc, NWQueries.Q31, "Q31", 830, totalProcessors,
          classOf[HashJoinExec])
        case "Q32" => NWQueries.assertJoin(snc, NWQueries.Q32, "Q32", 8, 8, classOf[HashJoinExec])
        case "Q33" => NWQueries.assertJoin(snc, NWQueries.Q33, "Q33", 37, totalProcessors,
          classOf[HashJoinExec])
        case "Q34" => NWQueries.assertJoin(snc, NWQueries.Q34, "Q34", 5, totalProcessors,
          classOf[HashJoinExec])
        case "Q35" => NWQueries.assertJoin(snc, NWQueries.Q35, "Q35", 3, 4, classOf[HashJoinExec])
        case "Q36" => NWQueries.assertJoin(snc, NWQueries.Q36, "Q36", 290, totalProcessors,
          classOf[HashJoinExec])
        case "Q37" => NWQueries.assertJoin(snc, NWQueries.Q37, "Q37", 77, totalProcessors,
          classOf[HashJoinExec])
        case "Q38" => NWQueries.assertJoin(snc, NWQueries.Q38, "Q38", 2155, totalProcessors,
          classOf[HashJoinExec])
        case "Q39" => NWQueries.assertJoin(snc, NWQueries.Q39, "Q39", 9, 12,
          classOf[HashJoinExec])
        case "Q40" => NWQueries.assertJoin(snc, NWQueries.Q40, "Q40", 830, totalProcessors,
          classOf[HashJoinExec])
        case "Q41" => NWQueries.assertJoin(snc, NWQueries.Q41, "Q41", 2155, 8,
          classOf[HashJoinExec])
        case "Q42" => NWQueries.assertJoin(snc, NWQueries.Q42, "Q42", 22, totalProcessors,
          classOf[HashJoinExec])
        case "Q43" => NWQueries.assertJoin(snc, NWQueries.Q43, "Q43", 830, 8,
          classOf[SortMergeJoinExec])
        case "Q44" => NWQueries.assertJoin(snc, NWQueries.Q44, "Q44", 830, 8,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q45" => NWQueries.assertJoin(snc, NWQueries.Q45, "Q45", 1788650, 8,
          classOf[CartesianProductExec])
        case "Q46" => NWQueries.assertJoin(snc, NWQueries.Q46, "Q46", 1788650, 8,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q47" => NWQueries.assertJoin(snc, NWQueries.Q47, "Q47", 1788650, 8,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q48" => NWQueries.assertJoin(snc, NWQueries.Q48, "Q48", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q49" => NWQueries.assertJoin(snc, NWQueries.Q49, "Q49", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q50" => NWQueries.assertJoin(snc, NWQueries.Q50, "Q50", 2155, 8,
          classOf[HashJoinExec])
        case "Q51" => NWQueries.assertJoin(snc, NWQueries.Q51, "Q51", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q52" => NWQueries.assertJoin(snc, NWQueries.Q52, "Q52", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q53" => NWQueries.assertJoin(snc, NWQueries.Q53, "Q53", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q54" => NWQueries.assertJoin(snc, NWQueries.Q54, "Q54", 2155, 8,
          classOf[SortMergeJoinExec])
        case "Q55" => NWQueries.assertJoin(snc, NWQueries.Q55, "Q55", 21, totalProcessors,
          classOf[HashJoinExec])
        case "Q56" => NWQueries.assertJoin(snc, NWQueries.Q56, "Q56", 8, 1, classOf[HashJoinExec])
      }
    }
  }

  private def validateColocatedTableQueries(snc: SnappyContext): Unit = {

    val numDefaultPartitions = ((totalProcessors - 4) to (totalProcessors + 4)).toArray
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => NWQueries.assertQuery(snc, NWQueries.Q1, "Q1", 8, 1, classOf[RowTableScan])
        case "Q2" => NWQueries.assertQuery(snc, NWQueries.Q2, "Q2", 91, numDefaultPartitions,
          classOf[ColumnTableScan])
        case "Q3" => NWQueries.assertQuery(snc, NWQueries.Q3, "Q3", 830, numDefaultPartitions,
          classOf[RowTableScan])
        case "Q4" => NWQueries.assertQuery(snc, NWQueries.Q4, "Q4", 9, 4, classOf[RowTableScan])
        case "Q5" => NWQueries.assertQuery(snc, NWQueries.Q5, "Q5", 9, 8, classOf[RowTableScan])
        case "Q6" => NWQueries.assertQuery(snc, NWQueries.Q6, "Q6", 9, 8, classOf[RowTableScan])
        case "Q7" => NWQueries.assertQuery(snc, NWQueries.Q7, "Q7", 9, 8, classOf[RowTableScan])
        case "Q8" => NWQueries.assertQuery(snc, NWQueries.Q8, "Q8", 6, 4, classOf[FilterExec])
        case "Q9" => NWQueries.assertQuery(snc, NWQueries.Q9, "Q9", 3, 4, classOf[ProjectExec])
        case "Q10" => NWQueries.assertQuery(snc, NWQueries.Q10, "Q10", 2, 4, classOf[FilterExec])
        case "Q11" => NWQueries.assertQuery(snc, NWQueries.Q11, "Q11", 4, 4, classOf[ProjectExec])
        case "Q12" => NWQueries.assertQuery(snc, NWQueries.Q12, "Q12", 2, 4, classOf[FilterExec])
        case "Q13" => NWQueries.assertQuery(snc, NWQueries.Q13, "Q13", 2, numDefaultPartitions,
          classOf[FilterExec])
        case "Q14" => NWQueries.assertQuery(snc, NWQueries.Q14, "Q14", 69, totalProcessors,
          classOf[FilterExec])
        case "Q15" => NWQueries.assertQuery(snc, NWQueries.Q15, "Q15", 5, 4, classOf[FilterExec])
        case "Q16" => NWQueries.assertQuery(snc, NWQueries.Q16, "Q16", 8, 4, classOf[FilterExec])
        case "Q17" => NWQueries.assertQuery(snc, NWQueries.Q17, "Q17", 3, 4, classOf[FilterExec])
        case "Q18" => NWQueries.assertQuery(snc, NWQueries.Q18, "Q18", 9, 4, classOf[ProjectExec])
        case "Q19" => NWQueries.assertQuery(snc, NWQueries.Q19, "Q19", 13, numDefaultPartitions,
          classOf[ProjectExec])
        case "Q20" => NWQueries.assertQuery(snc, NWQueries.Q20, "Q20", 1, 1, classOf[ProjectExec])
        case "Q21" => NWQueries.assertQuery(snc, NWQueries.Q21, "Q21", 1, 1, classOf[RowTableScan])
        case "Q22" => NWQueries.assertQuery(snc, NWQueries.Q22, "Q22", 1, 2, classOf[ProjectExec])
        case "Q23" => NWQueries.assertQuery(snc, NWQueries.Q23, "Q23", 1, 1, classOf[RowTableScan])
        case "Q24" => NWQueries.assertQuery(snc, NWQueries.Q24, "Q24", 4, 4, classOf[ProjectExec])
        case "Q25" => NWQueries.assertJoin(snc, NWQueries.Q25, "Q25", 1, 8,
          classOf[ColumnTableScan])
        case "Q26" => NWQueries.assertJoin(snc, NWQueries.Q26, "Q26", 86, 16,
          classOf[BroadcastHashJoinExec])
        case "Q27" => NWQueries.assertJoin(snc, NWQueries.Q27, "Q27", 9, 12,
          classOf[SortMergeJoinExec])
        case "Q28" => NWQueries.assertJoin(snc, NWQueries.Q28, "Q28", 12, totalProcessors,
          classOf[ColumnTableScan])
        case "Q29" => NWQueries.assertJoin(snc, NWQueries.Q29, "Q29", 8, 12,
          classOf[BroadcastHashJoinExec])
        case "Q30" => NWQueries.assertJoin(snc, NWQueries.Q30, "Q30", 8, 12,
          classOf[BroadcastHashJoinExec])
        case "Q31" => NWQueries.assertJoin(snc, NWQueries.Q31, "Q31", 830, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q32" => NWQueries.assertJoin(snc, NWQueries.Q32, "Q32", 8, 8,
          classOf[BroadcastHashJoinExec])
        case "Q33" => NWQueries.assertJoin(snc, NWQueries.Q33, "Q33", 37, 8,
          classOf[BroadcastHashJoinExec])
        case "Q34" => NWQueries.assertJoin(snc, NWQueries.Q34, "Q34", 5, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q35" => NWQueries.assertJoin(snc, NWQueries.Q35, "Q35", 3, 4,
          classOf[BroadcastHashJoinExec])
        case "Q36" => NWQueries.assertJoin(snc, NWQueries.Q36, "Q36", 290, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q37" => NWQueries.assertJoin(snc, NWQueries.Q37, "Q37", 77, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q38" => NWQueries.assertJoin(snc, NWQueries.Q38, "Q38", 2155, totalProcessors,
          classOf[HashJoinExec])
        case "Q39" => NWQueries.assertJoin(snc, NWQueries.Q39, "Q39", 9, 12,
          classOf[BroadcastHashJoinExec])
        case "Q40" => NWQueries.assertJoin(snc, NWQueries.Q40, "Q40", 830, 16,
          classOf[BroadcastHashJoinExec])
        case "Q41" => NWQueries.assertJoin(snc, NWQueries.Q41, "Q41", 2155, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q42" => NWQueries.assertJoin(snc, NWQueries.Q42, "Q42", 22, totalProcessors,
          classOf[BroadcastHashJoinExec])
        case "Q43" => NWQueries.assertJoin(snc, NWQueries.Q43, "Q43", 830, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q44" => NWQueries.assertJoin(snc, NWQueries.Q44, "Q44", 830, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q45" => NWQueries.assertJoin(snc, NWQueries.Q45, "Q45", 1788650, 16,
          classOf[CartesianProductExec])
        case "Q46" => NWQueries.assertJoin(snc, NWQueries.Q46, "Q46", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q47" => NWQueries.assertJoin(snc, NWQueries.Q47, "Q47", 1788650, 32,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q48" => NWQueries.assertJoin(snc, NWQueries.Q48, "Q48", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q49" => NWQueries.assertJoin(snc, NWQueries.Q49, "Q49", 1788650, 16,
          classOf[BroadcastNestedLoopJoinExec])
        case "Q50" => NWQueries.assertJoin(snc, NWQueries.Q50, "Q50", 2155, totalProcessors,
          classOf[HashJoinExec])
        case "Q51" => NWQueries.assertJoin(snc, NWQueries.Q51, "Q51", 2155, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q52" => NWQueries.assertJoin(snc, NWQueries.Q52, "Q52", 2155, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q53" => NWQueries.assertJoin(snc, NWQueries.Q53, "Q53", 2155, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q54" => NWQueries.assertJoin(snc, NWQueries.Q54, "Q54", 2155, totalProcessors,
          classOf[SortMergeJoinExec])
        case "Q55" => NWQueries.assertJoin(snc, NWQueries.Q55, "Q55", 21, totalProcessors,
          classOf[HashJoinExec])
        case "Q56" => NWQueries.assertJoin(snc, NWQueries.Q56, "Q56", 8, 1, classOf[HashJoinExec])
      }
    }
  }

  private def createAndLoadColumnTableUsingJDBC(stmt: Statement, snc: SnappyContext): Unit = {

    stmt.executeUpdate(NWQueries.products_table + " USING column options (" +
        "partition_by 'ProductID,SupplierID', buckets '4', redundancy '2')")
    NWQueries.products(snc).collect().foreach(row => {
      val colValues = row.toSeq
      val sqlQuery: String = s"INSERT INTO products VALUES(${colValues.head}, " +
          s"'${colValues(1).toString.replace("'", "")}',${colValues(2)}, ${colValues(3)}, " +
          s"'${colValues(4).toString.replace("'", "")}',${colValues(5)}, ${colValues(6)}, " +
          s"${colValues(7)}, ${colValues(8)},  ${colValues(9)})"
      stmt.executeUpdate(sqlQuery)
    })
  }
}

object NorthWindDUnitTest {

  def createAndLoadSparkTables(sqlContext: SQLContext): Unit = {
    NWQueries.regions(sqlContext).createOrReplaceTempView("regions")
    NWQueries.categories(sqlContext).createOrReplaceTempView("categories")
    NWQueries.shippers(sqlContext).createOrReplaceTempView("shippers")
    NWQueries.employees(sqlContext).createOrReplaceTempView("employees")
    NWQueries.customers(sqlContext).createOrReplaceTempView("customers")
    NWQueries.orders(sqlContext).createOrReplaceTempView("orders")
    NWQueries.order_details(sqlContext).createOrReplaceTempView("order_details")
    NWQueries.products(sqlContext).createOrReplaceTempView("products")
    NWQueries.suppliers(sqlContext).createOrReplaceTempView("suppliers")
    NWQueries.territories(sqlContext).createOrReplaceTempView("territories")
    NWQueries.employee_territories(sqlContext).createOrReplaceTempView("employee_territories")
  }

  def createAndLoadReplicatedTables(snc: SnappyContext): Unit = {
    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers(snc).write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table)
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table)
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table)
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table)
    NWQueries.suppliers(snc).write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table)
    NWQueries.territories(snc).write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table)
    NWQueries.employee_territories(snc).write.insertInto("employee_territories")
  }

  def createAndLoadColumnTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers(snc).write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table)
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using column options (" +
        "partition_by 'OrderId', buckets '8', redundancy '1')")
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table + " using column options (" +
        "partition_by 'OrderId', buckets '8', COLOCATE_WITH 'orders', " +
        "redundancy '1')")
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " using column options ( partition_by 'ProductID', buckets '16')")
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '12' )")
    NWQueries.suppliers(snc).write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using column options (partition_by 'TerritoryID', buckets '4')")
    NWQueries.territories(snc).write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table +
        " using column options(partition_by 'EmployeeID', buckets '1')")
    NWQueries.employee_territories(snc).write.insertInto("employee_territories")
  }

  def createAndLoadColocatedTables(snc: SnappyContext): Unit = {

    snc.sql(NWQueries.regions_table)
    NWQueries.regions(snc).write.insertInto("regions")

    snc.sql(NWQueries.categories_table)
    NWQueries.categories(snc).write.insertInto("categories")

    snc.sql(NWQueries.shippers_table)
    NWQueries.shippers(snc).write.insertInto("shippers")

    snc.sql(NWQueries.employees_table)
    NWQueries.employees(snc).write.insertInto("employees")

    snc.sql(NWQueries.customers_table + " using column options(" +
        "partition_by 'CustomerID', buckets '8', redundancy '1')")
    NWQueries.customers(snc).write.insertInto("customers")

    snc.sql(NWQueries.orders_table + " using row options (" +
        "partition_by 'CustomerID', buckets '8', " +
        "colocate_with 'customers', redundancy '1')")
    NWQueries.orders(snc).write.insertInto("orders")

    snc.sql(NWQueries.order_details_table + " using row options (" +
        "partition_by 'ProductID', buckets '16', redundancy '1')")
    NWQueries.order_details(snc).write.insertInto("order_details")

    snc.sql(NWQueries.products_table +
        " USING column options ( partition_by 'ProductID', buckets '16'," +
        " colocate_with 'order_details', redundancy '1')")
    NWQueries.products(snc).write.insertInto("products")

    snc.sql(NWQueries.suppliers_table +
        " USING column options (PARTITION_BY 'SupplierID', buckets '12')")
    NWQueries.suppliers(snc).write.insertInto("suppliers")

    snc.sql(NWQueries.territories_table +
        " using column options (partition_by 'TerritoryID', buckets '4')")
    NWQueries.territories(snc).write.insertInto("territories")

    snc.sql(NWQueries.employee_territories_table + " using row options(" +
        "partition_by 'TerritoryID', buckets '4', colocate_with 'territories')")
    NWQueries.employee_territories(snc).write.insertInto("employee_territories")
  }

  protected def getTempDir(dirName: String, onlyOnce: Boolean): String = {
    var log: File = new File(".")
    if (onlyOnce) {
      val logParent = log.getAbsoluteFile.getParentFile.getParentFile
      if (logParent.list().contains("output.txt")) {
        log = logParent
      } else if (logParent.getParentFile.list().contains("output.txt")) {
        log = logParent.getParentFile
      }
    }
    var dest: String = null
    dest = log.getCanonicalPath + File.separator + dirName
    val tempDir: File = new File(dest)
    if (!tempDir.exists) tempDir.mkdir()
    tempDir.getAbsolutePath
  }

  private def getSortedFiles(file: File): Array[File] = {
    file.getParentFile.listFiles.filter(_.getName.startsWith(file.getName)).sortBy { f =>
      val n = f.getName
      val i = n.lastIndexOf('.')
      n.substring(i + 1).toInt
    }
  }

  def assertQueryFullResultSet(snc: SnappyContext, sqlString: String, numRows: Int,
      queryNum: String, tableType: String, pw: PrintWriter, sqlContext: SQLContext): Any = {
    var snappyDF = snc.sql(sqlString)
    val snappyQueryFileName = s"Snappy_$queryNum.out"
    val sparkQueryFileName = s"Spark_$queryNum.out"
    val snappyDest = getTempDir("snappyQueryFiles_" + tableType, onlyOnce = false)
    val sparkDest = getTempDir("sparkQueryFiles", onlyOnce = true)
    val sparkFile = new File(sparkDest, sparkQueryFileName)
    val snappyFile = new File(snappyDest, snappyQueryFileName)
    val col1 = snappyDF.schema.fieldNames(0)
    val col = snappyDF.schema.fieldNames.tail
    snappyDF = snappyDF.sort(col1, col: _*)
    writeToFile(snappyDF, snappyFile, snc)
    // scalastyle:off println
    pw.println(s"$queryNum Result Collected in files with prefix $snappyFile")
    if (!new File(s"$sparkFile.0").exists()) {
      var sparkDF = sqlContext.sql(sqlString)
      val col = sparkDF.schema.fieldNames(0)
      val cols = sparkDF.schema.fieldNames.tail
      sparkDF = sparkDF.sort(col, cols: _*)
      writeToFile(sparkDF, sparkFile, snc)
      pw.println(s"$queryNum Result Collected in files with prefix $sparkFile")
    }
    val expectedFiles = getSortedFiles(sparkFile).toIterator
    val actualFiles = getSortedFiles(snappyFile).toIterator
    val expectedLineSet = expectedFiles.flatMap(Source.fromFile(_).getLines())
    val actualLineSet = actualFiles.flatMap(Source.fromFile(_).getLines())
    var numLines = 0
    while (expectedLineSet.hasNext && actualLineSet.hasNext) {
      val expectedLine = expectedLineSet.next()
      val actualLine = actualLineSet.next()
      if (!actualLine.equals(expectedLine)) {
        pw.println(s"\n** For $queryNum result mismatch observed**")
        pw.println(s"\nExpected Result \n: $expectedLine")
        pw.println(s"\nActual Result   \n: $actualLine")
        pw.println(s"\nQuery =" + sqlString + " Table Type : " + tableType)
        assert(assertion = false, s"\n** For $queryNum result mismatch observed** \n" +
            s"Expected Result \n: $expectedLine \n" +
            s"Actual Result   \n: $actualLine \n" +
            s"Query =" + sqlString + " Table Type : " + tableType)
      }
      numLines += 1
    }
    if (actualLineSet.hasNext || expectedLineSet.hasNext) {
      pw.println(s"\nFor $queryNum result count mismatch observed")
      assert(assertion = false, s"\nFor $queryNum result count mismatch observed")
    }
    assert(numLines == numRows, s"\nFor $queryNum result count mismatch " +
        s"observed: Expected=$numRows, Got=$numLines")
    // scalastyle:on println
    pw.flush()
  }

  def assertJoinFullResultSet(snc: SnappyContext, sqlString: String, numRows: Int,
      queryNum: String, tableType: String, pw: PrintWriter, sqlContext: SQLContext): Any = {
    snc.sql("set spark.sql.crossJoin.enabled = true")
    sqlContext.sql("set spark.sql.crossJoin.enabled = true")
    assertQueryFullResultSet(snc, sqlString, numRows, queryNum, tableType, pw, sqlContext)
  }

  def writeToFile(df: DataFrame, dest: File, snc: SnappyContext): Unit = {
    val parent = dest.getParentFile
    if (!parent.exists()) {
      parent.mkdirs()
    }
    val destFile = dest.getAbsolutePath
    implicit val encoder = RowEncoder(df.schema)
    df.mapPartitions { iter =>
      val sb = new StringBuilder
      val partitionId = TaskContext.getPartitionId()
      val pw = new PrintWriter(s"$destFile.$partitionId")
      try {
        iter.foreach { row =>
          row.toSeq.foreach {
            case d: Double =>
              // round to one decimal digit
              sb.append(math.floor(d * 5.0 + 0.25) / 5.0).append(',')
            case bd: java.math.BigDecimal =>
              sb.append(bd.setScale(2, java.math.RoundingMode.HALF_UP)).append(',')
            case v => sb.append(v).append(',')
          }
          val len = sb.length
          if (len > 0) sb.setLength(len - 1)
          sb.append('\n')
          if (sb.length >= 1048576) {
            pw.append(sb)
            pw.flush()
            sb.clear()
          }
        }
        if (sb.nonEmpty) {
          pw.append(sb)
          pw.flush()
        }
      } finally {
        pw.close()
      }
      Iterator.empty
    }.collect()
  }

  def validateQueriesFullResultSet(snc: SnappyContext, tableType: String,
      pw: PrintWriter, sqlContext: SQLContext): Unit = {
    for (q <- NWQueries.queries) {
      q._1 match {
        case "Q1" => assertQueryFullResultSet(snc,
          NWQueries.Q1, 8, "Q1", tableType, pw, sqlContext)
        case "Q2" => assertQueryFullResultSet(snc,
          NWQueries.Q2, 91, "Q2", tableType, pw, sqlContext)
        case "Q3" => assertQueryFullResultSet(snc,
          NWQueries.Q3, 830, "Q3", tableType, pw, sqlContext)
        case "Q4" => assertQueryFullResultSet(snc,
          NWQueries.Q4, 9, "Q4", tableType, pw, sqlContext)
        case "Q5" => assertQueryFullResultSet(snc,
          NWQueries.Q5, 9, "Q5", tableType, pw, sqlContext)
        case "Q6" => assertQueryFullResultSet(snc,
          NWQueries.Q6, 9, "Q6", tableType, pw, sqlContext)
        case "Q7" => assertQueryFullResultSet(snc,
          NWQueries.Q7, 9, "Q7", tableType, pw, sqlContext)
        case "Q8" => assertQueryFullResultSet(snc,
          NWQueries.Q8, 6, "Q8", tableType, pw, sqlContext)
        case "Q9" => assertQueryFullResultSet(snc,
          NWQueries.Q9, 3, "Q9", tableType, pw, sqlContext)
        case "Q10" => assertQueryFullResultSet(snc,
          NWQueries.Q10, 2, "Q10", tableType, pw, sqlContext)
        case "Q11" => assertQueryFullResultSet(snc,
          NWQueries.Q11, 4, "Q11", tableType, pw, sqlContext)
        case "Q12" => assertQueryFullResultSet(snc,
          NWQueries.Q12, 2, "Q12", tableType, pw, sqlContext)
        case "Q13" => assertQueryFullResultSet(snc,
          NWQueries.Q13, 2, "Q13", tableType, pw, sqlContext)
        case "Q14" => assertQueryFullResultSet(snc,
          NWQueries.Q14, 69, "Q14", tableType, pw, sqlContext)
        case "Q15" => assertQueryFullResultSet(snc,
          NWQueries.Q15, 5, "Q15", tableType, pw, sqlContext)
        case "Q16" => assertQueryFullResultSet(snc,
          NWQueries.Q16, 8, "Q16", tableType, pw, sqlContext)
        case "Q17" => assertQueryFullResultSet(snc,
          NWQueries.Q17, 3, "Q17", tableType, pw, sqlContext)
        case "Q18" => assertQueryFullResultSet(snc,
          NWQueries.Q18, 9, "Q18", tableType, pw, sqlContext)
        case "Q19" => assertQueryFullResultSet(snc,
          NWQueries.Q19, 13, "Q19", tableType, pw, sqlContext)
        case "Q20" => assertQueryFullResultSet(snc,
          NWQueries.Q20, 1, "Q20", tableType, pw, sqlContext)
        case "Q21" => assertQueryFullResultSet(snc,
          NWQueries.Q21, 1, "Q21", tableType, pw, sqlContext)
        case "Q22" => assertQueryFullResultSet(snc,
          NWQueries.Q22, 1, "Q22", tableType, pw, sqlContext)
        case "Q23" => assertQueryFullResultSet(snc,
          NWQueries.Q23, 1, "Q23", tableType, pw, sqlContext)
        case "Q24" => assertQueryFullResultSet(snc,
          NWQueries.Q24, 4, "Q24", tableType, pw, sqlContext)
        case "Q25" => assertJoinFullResultSet(snc,
          NWQueries.Q25, 1, "Q25", tableType, pw, sqlContext)
        /*
        case "Q25_1" => assertJoinFullResultSet(snc,
          NWQueries.Q25_1, 1, "Q25_1", tableType, pw, sqlContext)
        case "Q25_2" => assertJoinFullResultSet(snc,
          NWQueries.Q25_2, 1, "Q25_2", tableType, pw, sqlContext)
        */
        case "Q26" => assertJoinFullResultSet(snc,
          NWQueries.Q26, 86, "Q26", tableType, pw, sqlContext)
        /*
        case "Q26_1" => assertJoinFullResultSet(snc,
          NWQueries.Q26_1, 54, "Q26_1", tableType, pw, sqlContext)
        case "Q26_2" => assertJoinFullResultSet(snc,
          NWQueries.Q26_2, 60, "Q26_2", tableType, pw, sqlContext)
        */
        case "Q27" => assertJoinFullResultSet(snc,
          NWQueries.Q27, 9, "Q27", tableType, pw, sqlContext)
        /*
        case "Q27_1" => assertJoinFullResultSet(snc,
          NWQueries.Q27_1, 5, "Q27_1", tableType, pw, sqlContext)
        case "Q27_2" => assertJoinFullResultSet(snc,
          NWQueries.Q27_2, 8, "Q27_2", tableType, pw, sqlContext)
        case "Q27_3" => assertJoinFullResultSet(snc,
          NWQueries.Q27_3, 3, "Q27_3", tableType, pw, sqlContext)
        case "Q27_4" => assertJoinFullResultSet(snc,
          NWQueries.Q27_4, 6, "Q27_4", tableType, pw, sqlContext)
        */
        case "Q28" => assertJoinFullResultSet(snc,
          NWQueries.Q28, 12, "Q28", tableType, pw, sqlContext)
        /*
        case "Q28_1" => assertJoinFullResultSet(snc,
          NWQueries.Q28_1, 12, "Q28_1", tableType, pw, sqlContext)
        case "Q28_2" => assertJoinFullResultSet(snc,
          NWQueries.Q28_2, 5, "Q28_2", tableType, pw, sqlContext)
        */
        case "Q29" => assertJoinFullResultSet(snc,
          NWQueries.Q29, 8, "Q29", tableType, pw, sqlContext)
        /*
        case "Q29_1" => assertJoinFullResultSet(snc,
          NWQueries.Q29_1, 5, "Q29_1", tableType, pw, sqlContext)
        case "Q29_2" => assertJoinFullResultSet(snc,
          NWQueries.Q29_2, 6, "Q29_2", tableType, pw, sqlContext)
        */
        case "Q30" => assertJoinFullResultSet(snc,
          NWQueries.Q30, 8, "Q30", tableType, pw, sqlContext)
        /*
        case "Q30_1" => assertJoinFullResultSet(snc,
          NWQueries.Q30_1, 8, "Q30_1", tableType, pw, sqlContext)
        case "Q30_2" => assertJoinFullResultSet(snc,
          NWQueries.Q30_2, 6, "Q30_2", tableType, pw, sqlContext)
        */
        case "Q31" => assertJoinFullResultSet(snc,
          NWQueries.Q31, 830, "Q31", tableType, pw, sqlContext)
        /*
        case "Q31_1" => assertJoinFullResultSet(snc,
          NWQueries.Q31_1, 502, "Q31_1", tableType, pw, sqlContext)
        case "Q31_2" => assertJoinFullResultSet(snc,
          NWQueries.Q31_2, 286, "Q31_2", tableType, pw, sqlContext)
        case "Q31_3" => assertJoinFullResultSet(snc,
          NWQueries.Q31_3, 219, "Q31_3", tableType, pw, sqlContext)
        case "Q31_4" => assertJoinFullResultSet(snc,
          NWQueries.Q31_4, 484, "Q31_4", tableType, pw, sqlContext)
        */
        case "Q32" => assertJoinFullResultSet(snc,
          NWQueries.Q32, 8, "Q32", tableType, pw, sqlContext)
        /*
        case "Q32_1" => assertJoinFullResultSet(snc,
          NWQueries.Q32_1, 282, "Q32_1", tableType, pw, sqlContext)
        */
        case "Q33" => assertJoinFullResultSet(snc,
          NWQueries.Q33, 37, "Q33", tableType, pw, sqlContext)
        /*
        case "Q33_1" => assertJoinFullResultSet(snc,
          NWQueries.Q33_1, 769, "Q33_1", tableType, pw, sqlContext)
        */
        case "Q34" => assertJoinFullResultSet(snc,
          NWQueries.Q34, 5, "Q34", tableType, pw, sqlContext)
        /*
        case "Q34_1" => assertJoinFullResultSet(snc,
          NWQueries.Q34_1, 1, "Q34_1", tableType, pw, sqlContext)
        case "Q34_2" => assertJoinFullResultSet(snc,
          NWQueries.Q34_2, 4, "Q34_2", tableType, pw, sqlContext)
        */
        case "Q35" => assertJoinFullResultSet(snc,
          NWQueries.Q35, 3, "Q35", tableType, pw, sqlContext)
        /*
        case "Q35_1" => assertJoinFullResultSet(snc,
          NWQueries.Q35_1, 2, "Q35_1", tableType, pw, sqlContext)
        case "Q35_2" => assertJoinFullResultSet(snc,
          NWQueries.Q35_2, 3, "Q35_2", tableType, pw, sqlContext)
        */
        case "Q36" => assertJoinFullResultSet(snc,
          NWQueries.Q36, 290, "Q36", tableType, pw, sqlContext)
        /*
        case "Q36_1" => assertJoinFullResultSet(snc,
          NWQueries.Q36_1, 232, "Q36_1", tableType, pw, sqlContext)
        case "Q36_2" => assertJoinFullResultSet(snc,
          NWQueries.Q36_2, 61, "Q36_2", tableType, pw, sqlContext)
        */
        case "Q37" => /* assertJoinFullResultSet(snc,
          NWQueries.Q37, 77, "Q37", tableType, pw, sqlContext) */
        case "Q38" => assertJoinFullResultSet(snc,
          NWQueries.Q38, 2155, "Q38", tableType, pw, sqlContext)
        /*
        case "Q38_1" => assertJoinFullResultSet(snc,
          NWQueries.Q38_1, 2080, "Q38_1", tableType, pw, sqlContext)
        case "Q38_2" => assertJoinFullResultSet(snc,
          NWQueries.Q38_2, 2041, "Q38_2", tableType, pw, sqlContext)
        */
        case "Q39" => assertJoinFullResultSet(snc,
          NWQueries.Q39, 9, "Q39", tableType, pw, sqlContext)
        case "Q40" => assertJoinFullResultSet(snc,
          NWQueries.Q40, 830, "Q40", tableType, pw, sqlContext)
        /*
        case "Q40_1" => assertJoinFullResultSet(snc,
          NWQueries.Q40_1, 12, "Q40_1", tableType, pw, sqlContext)
        case "Q40_2" => assertJoinFullResultSet(snc,
          NWQueries.Q40_2, 9, "Q40_2", tableType, pw, sqlContext)
        */
        case "Q41" => assertJoinFullResultSet(snc,
          NWQueries.Q41, 2155, "Q41", tableType, pw, sqlContext)
        case "Q42" => assertJoinFullResultSet(snc,
          NWQueries.Q42, 22, "Q42", tableType, pw, sqlContext)
        /*
        case "Q42_1" => assertJoinFullResultSet(snc,
          NWQueries.Q42_1, 22, "Q42_1", tableType, pw, sqlContext)
        case "Q42_2" => assertJoinFullResultSet(snc,
          NWQueries.Q42_2, 7, "Q42_2", tableType, pw, sqlContext)
        */
        case "Q43" => assertJoinFullResultSet(snc,
          NWQueries.Q43, 830, "Q43", tableType, pw, sqlContext)
        /*
        case "Q43_1" => assertJoinFullResultSet(snc,
          NWQueries.Q43_1, 10, "Q43_1", tableType, pw, sqlContext)
        case "Q43_2" => assertJoinFullResultSet(snc,
          NWQueries.Q43_2, 2, "Q43_2", tableType, pw, sqlContext)
        */
        case "Q44" => assertJoinFullResultSet(snc,
          NWQueries.Q44, 830, "Q44", tableType, pw, sqlContext)
        case "Q45" => assertJoinFullResultSet(snc,
          NWQueries.Q45, 1788650, "Q45", tableType, pw, sqlContext)
        case "Q46" => assertJoinFullResultSet(snc,
          NWQueries.Q46, 1788650, "Q46", tableType, pw, sqlContext)
        case "Q47" => assertJoinFullResultSet(snc,
          NWQueries.Q47, 1788650, "Q47", tableType, pw, sqlContext)
        case "Q48" => assertJoinFullResultSet(snc,
          NWQueries.Q48, 1788650, "Q48", tableType, pw, sqlContext)
        case "Q49" => assertJoinFullResultSet(snc,
          NWQueries.Q49, 1788650, "Q49", tableType, pw, sqlContext)
        /*
        case "Q49_1" => assertJoinFullResultSet(snc,
          NWQueries.Q49_1, 1713225, "Q49_1", tableType, pw, sqlContext)
        case "Q49_2" => assertJoinFullResultSet(snc,
          NWQueries.Q49_2, 1741240, "Q49_2", tableType, pw, sqlContext)
        */
        case "Q50" => assertJoinFullResultSet(snc,
          NWQueries.Q50, 2155, "Q50", tableType, pw, sqlContext)
        case "Q51" => assertJoinFullResultSet(snc,
          NWQueries.Q51, 2155, "Q51", tableType, pw, sqlContext)
        /*
        case "Q51_1" => assertJoinFullResultSet(snc,
          NWQueries.Q51_1, 2080, "Q51_1", tableType, pw, sqlContext)
        case "Q51_2" => assertJoinFullResultSet(snc,
          NWQueries.Q51_2, 2041, "Q51_2", tableType, pw, sqlContext)
        */
        case "Q52" => assertJoinFullResultSet(snc,
          NWQueries.Q52, 2155, "Q52", tableType, pw, sqlContext)
        case "Q53" => assertJoinFullResultSet(snc,
          NWQueries.Q53, 2155, "Q53", tableType, pw, sqlContext)
        case "Q54" => assertJoinFullResultSet(snc,
          NWQueries.Q54, 2155, "Q54", tableType, pw, sqlContext)
        case "Q55" => assertJoinFullResultSet(snc,
          NWQueries.Q55, 21, "Q55", tableType, pw, sqlContext)
        /*
        case "Q55_1" => assertJoinFullResultSet(snc,
          NWQueries.Q55_1, 7, "Q55_1", tableType, pw, sqlContext)
        case "Q55_2" => assertJoinFullResultSet(snc,
          NWQueries.Q55_2, 6, "Q55_2", tableType, pw, sqlContext)
        */
        case "Q56" => assertJoinFullResultSet(snc,
          NWQueries.Q56, 8, "Q56", tableType, pw, sqlContext)
        /*
        case "Q56_1" => assertJoinFullResultSet(snc,
          NWQueries.Q56, 8, "Q56_1", tableType, pw, sqlContext)
        case "Q56_2" => assertJoinFullResultSet(snc,
          NWQueries.Q56, 8, "Q56_2", tableType, pw, sqlContext)
        case "Q56_3" => assertJoinFullResultSet(snc,
          NWQueries.Q56, 8, "Q56_3", tableType, pw, sqlContext)
        */
        case _ =>
          // scalastyle:off println
          println("OK")
        // scalastyle:on println
      }
    }
  }
}
