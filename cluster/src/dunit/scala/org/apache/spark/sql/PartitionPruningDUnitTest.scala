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

import io.snappydata.benchmark.TPCHColumnPartitionedTable
import io.snappydata.cluster.ClusterManagerTestBase
import org.apache.spark.rdd.ZippedPartitionsPartition
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.collection.MultiBucketExecutorPartition
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.row.RowTableScan


class PartitionPruningDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  val query1 = "select * from orders where o_orderkey = "

  val query2 = "select * from orders where o_orderkey = {fn substring('d1xxd2', 2, 1)} "

  val query3 = "select * from orders where o_orderkey = substring('acbc801xx', 5, 3) "

  val query4 = "select * from orders where o_orderkey = {fn trim(" +
    "substring(' acbc801xx', length(' 12345'), length('801'))) }"

  val query5 = "select * from orders where o_orderkey = trim(" +
    "substring(' acbc1410xx', length(' 12345'), length('1410'))) "

  val query6 = "select O_ORDERDATE, {fn TIMESTAMPADD(SQL_TSI_DAY," +
    " {fn FLOOR((-1 * {fn DAYOFYEAR(O_ORDERDATE)} - 1))}, O_ORDERDATE)}" +
    " from orders where O_ORDERKEY = 32"

  def testRowTablePruning(): Unit = {

    val snc = SnappyContext(sc)

    val tpchDataPath = TPCHColumnPartitionedTable.getClass.getResource("/TPCH").getPath
    val buckets_Order_Lineitem = "5"
    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath,
      isSnappy = true, buckets_Order_Lineitem, null, provider = "row")

    def validateSinglePartition(df: DataFrame, bucketId: Int): Unit = {
      val plan = df.queryExecution.executedPlan.collectFirst {
        case c: RowTableScan => c
      }

      val scanRDD = plan.map(_.dataRDD).
        getOrElse(throw new AssertionError("Expecting RowTable Scan"))
      val partitions = scanRDD.partitions
      assert(plan.get.outputPartitioning == SinglePartition)
      assert(partitions.length == 1, {
        val sb = new StringBuilder("Pruning not in effect ? partitions found ")
        partitions.foreach(p => sb.append(p.index).append(","))
        sb.toString
      })
      val bstr = partitions(0) match {
        case zp: ZippedPartitionsPartition => zp.partitionValues.map {
          case mb: MultiBucketExecutorPartition => mb.bucketsString
        }
        case _ => Nil
      }

      // each BucketExecutor must have only one bucket.
      // there are 2 BucketExecutor entries due to ZipPartion of RowBuffer.
      assert(bstr.forall(_.toInt == bucketId), s"Expected $bucketId, found $bstr")
    }

    validateSinglePartition(executeQuery(snc, query1 + 1, 1), 4)
    validateSinglePartition(executeQuery(snc, query1 + 32, 32), 0)
    validateSinglePartition(executeQuery(snc, query1 + 801, 801), 4)
    // repeating the query deliberately
    validateSinglePartition(executeQuery(snc, query1 + 801, 801), 4)
    validateSinglePartition(executeQuery(snc, query1 + 1408, 1408), 0)
    validateSinglePartition(executeQuery(snc, query1 + 1409, 1409), 2)
    validateSinglePartition(executeQuery(snc, query1 + 1410, 1410), 0)
    validateSinglePartition(executeQuery(snc, query1 + 1796, 1796), 4)
    validateSinglePartition(executeQuery(snc, query1 + 801, 801), 4)
    executeQuery(snc, query1 + "'1'", 1)
    executeQuery(snc, query1 + "'32'", 32)
    executeQuery(snc, query2, 1)
    executeQuery(snc, query3, 801)
    executeQuery(snc, query4, 801)
    executeQuery(snc, query5, 1410)

    val df = executeQuery(snc, query6, 32, false)
    val r = df.collect()(0)
    assert(r.getDate(0).toString.equals("1995-07-16"))
    assert(r.getDate(1).toString.equals("1994-12-30"))

  }

  def testColumnTablePruning(): Unit = {

    val snc = SnappyContext(sc)
    // val earlierValue = io.snappydata.Property.ColumnBatchSize.get(snc.sessionState.conf)
    val tpchDataPath = TPCHColumnPartitionedTable.getClass.getResource("/TPCH").getPath
    val buckets_Order_Lineitem = "5"
    // io.snappydata.Property.ColumnBatchSize.set(snc.sessionState.conf, "1000")
    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath,
      isSnappy = true, buckets_Order_Lineitem, null, provider = "column")

    def validateSinglePartition(df: DataFrame, bucketId: Int): Unit = {
      val plan = df.queryExecution.executedPlan.collectFirst {
        case c: ColumnTableScan => c
      }

      val scanRDD = plan.map(_.dataRDD).
        getOrElse(throw new AssertionError("Expecting ColumnTable Scan"))
      val partitions = scanRDD.partitions
      assert(plan.get.outputPartitioning == SinglePartition)
      assert(partitions.length == 1, {
        val sb = new StringBuilder("Pruning not in effect ? partitions found ")
        partitions.foreach(p => sb.append(p.index).append(","))
        sb.toString
      })
      val bstr = partitions(0) match {
        case zp: ZippedPartitionsPartition => zp.partitionValues.map {
          case mb: MultiBucketExecutorPartition => mb.bucketsString
        }
        case _ => Nil
      }

      // each BucketExecutor must have only one bucket.
      // there are 2 BucketExecutor entries due to ZipPartion of RowBuffer.
      assert(bstr.forall(_.toInt == bucketId), s"Expected $bucketId, found $bstr")


      val metrics = df.queryExecution.executedPlan.collectLeaves().head
        .metrics
        .filterKeys(k =>
          k.equals("columnBatchesSeen") ||
            k.equals("columnBatchesSkipped")
        ).toList

      assert(metrics.head._2.value - metrics(1)._2.value == 1,
        s"Stats Predicate filter not applied during scan ? \n" +
          s" difference between" +
          s" ${metrics.map(a => s"${a._2.value} (${a._1})").mkString(" and ")}" +
          s" is expected to be exactly 1.")
    }

    validateSinglePartition(executeQuery(snc, query1 + 1, 1), 4)
    validateSinglePartition(executeQuery(snc, query1 + 32, 32), 0)
    validateSinglePartition(executeQuery(snc, query1 + 801, 801), 4)
    // repeating the query deliberately
    validateSinglePartition(executeQuery(snc, query1 + 801, 801), 4)
    validateSinglePartition(executeQuery(snc, query1 + 1408, 1408), 0)
    validateSinglePartition(executeQuery(snc, query1 + 1409, 1409), 2)
    validateSinglePartition(executeQuery(snc, query1 + 1410, 1410), 0)
    validateSinglePartition(executeQuery(snc, query1 + 1796, 1796), 4)
    validateSinglePartition(executeQuery(snc, query1 + 801, 801), 4)
    executeQuery(snc, query1 + "'1'", 1)
    executeQuery(snc, query1 + "'32'", 32)
    executeQuery(snc, query2, 1)
    executeQuery(snc, query3, 801)
    executeQuery(snc, query4, 801)
    executeQuery(snc, query5, 1410)

    val df = executeQuery(snc, query6, 32, false)
    val r = df.collect()(0)
    assert(r.getDate(0).toString.equals("1995-07-16"))
    assert(r.getDate(1).toString.equals("1994-12-30"))
    // io.snappydata.Property.ColumnBatchSize.set(snc.sessionState.conf, earlierValue)
  }

  private def executeQuery(snc: SnappyContext, sql: String, orderKey: Int,
                           doAssert: Boolean = true): DataFrame = {
    val df = snc.sql(sql)
    if (doAssert) assert(df.collect()(0).getLong(0) == orderKey)
    df
  }

}
