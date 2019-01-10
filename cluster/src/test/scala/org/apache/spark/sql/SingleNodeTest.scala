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
import io.snappydata.{PlanTest, SnappyFunSuite}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.rdd.ZippedPartitionsPartition
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.collection.MultiBucketExecutorPartition
import org.apache.spark.sql.execution.columnar.ColumnTableScan

class SingleNodeTest extends SnappyFunSuite with PlanTest with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    // System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.sql.codegen.comments", "true")
    System.setProperty("spark.testing", "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.sql.codegen.comments")
    System.clearProperty("spark.testing")
    super.afterAll()
  }

  test("Nodes Pruning") {
    val earlierValue = io.snappydata.Property.ColumnBatchSize.get(snc.sessionState.conf)
    try {
      io.snappydata.Property.ColumnBatchSize.set(snc.sessionState.conf, "1000")
      SingleNodeTest.testNodesPruning(snc)
    } finally {
      io.snappydata.Property.ColumnBatchSize.set(snc.sessionState.conf, earlierValue)
    }
  }

  test("case when generation") {

    snc.sql("create table czec1(c1 varchar(10), c2 integer) using column")
    (1 until 10).foreach(v => snc.sql(s"insert into czec1 values('$v', $v)"))

    val expected = Set("[3,3,3]",
      "[2,Other,2]",
      "[5,Other,5]"
    )

    val found = snc.sql(
        "SELECT \"CZECB\".\"C1\" AS \"CB\"," +
        "  (CASE \"CZECB\".\"C1\" WHEN '3' THEN '3' ELSE 'Other' END) AS \"CB__group_\"," +
        "  SUM(\"CZECB\".\"C2\") AS \"sum_CC_ok\" " +
        "FROM \"APP\".\"CZEC1\" \"CZECB\"" +
        "WHERE ((\"CZECB\".\"C1\" IN ('2', '3', '5')) AND " +
        "((\"CZECB\".\"C1\" IS NULL) OR " +
        "(NOT ({fn LOCATE('1',{fn LCASE(\"CZECB\".\"C1\")},1)} > 0)))) " +
        "GROUP BY 2, 1"
    ).collect().map(_.toString).toSet

    assert(expected.equals(found))
  }
}


object SingleNodeTest {
  def testNodesPruning(snc: SnappyContext): Unit = {
    // scalastyle:off println
    val tpchDataPath = TPCHColumnPartitionedTable.getClass.getResource("/TPCH").getPath
    val buckets_Order_Lineitem = "5"
    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath,
      isSnappy = true, buckets_Order_Lineitem, null)

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

    var df = snc.sql("select * from orders where o_orderkey = 1 ")
    assert(df.collect()(0).getLong(0) == 1)
    validateSinglePartition(df, 4)

    df = snc.sql("select * from orders where o_orderkey = 32 ")
    assert(df.collect()(0).getLong(0) == 32)
    validateSinglePartition(df, 0)

    df = snc.sql("select * from orders where o_orderkey = 801 ")
    assert(df.collect()(0).getLong(0) == 801)
    validateSinglePartition(df, 4)

    // repeating the query deliberately
    df = snc.sql("select * from orders where o_orderkey = 801 ")
    assert(df.collect()(0).getLong(0) == 801)
    validateSinglePartition(df, 4)

    df = snc.sql("select * from orders where o_orderkey = 1408 ")
    assert(df.collect()(0).getLong(0) == 1408)
    validateSinglePartition(df, 0)

    df = snc.sql("select * from orders where o_orderkey = 1409 ")
    assert(df.collect()(0).getLong(0) == 1409)
    validateSinglePartition(df, 2)

    df = snc.sql("select * from orders where o_orderkey = 1410 ")
    assert(df.collect()(0).getLong(0) == 1410)
    validateSinglePartition(df, 0)

    df = snc.sql("select * from orders where o_orderkey = 1796 ")
    assert(df.collect()(0).getLong(0) == 1796)
    validateSinglePartition(df, 4)

    df = snc.sql("select * from orders where o_orderkey = 801 ")
    assert(df.collect()(0).getLong(0) == 801)
    validateSinglePartition(df, 4)

    df = snc.sql("select * from orders where o_orderkey = '1' ")
    assert(df.collect()(0).getLong(0) == 1)
    // validateSinglePartition(df, 4) investigate why cast(order_key as double) gets introduced.

    df = snc.sql("select * from orders where o_orderkey = '32' ")
    assert(df.collect()(0).getLong(0) == 32)

    df = snc.sql("select * from orders where o_orderkey = {fn substring('d1xxd2', 2, 1)} ")
    assert(df.collect()(0).getLong(0) == 1)

    df = snc.sql("select * from orders where o_orderkey = substring('acbc801xx', 5, 3) ")
    assert(df.collect()(0).getLong(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = {fn trim(" +
        "substring(' acbc801xx', length(' 12345'), length('801'))) }")
    assert(df.collect()(0).getLong(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = trim(" +
        "substring(' acbc1410xx', length(' 12345'), length('1410'))) ")
    assert(df.collect()(0).getLong(0) == 1410)

    df = snc.sql("select O_ORDERDATE, {fn TIMESTAMPADD(SQL_TSI_DAY," +
        " {fn FLOOR((-1 * {fn DAYOFYEAR(O_ORDERDATE)} - 1))}, O_ORDERDATE)}" +
        " from orders where O_ORDERKEY = 32")
    val r = df.collect()(0)
    assert(r.getDate(0).toString.equals("1995-07-16"))
    assert(r.getDate(1).toString.equals("1994-12-30"))
    // scalastyle:on println
  }
}
