/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import com.pivotal.gemfirexd.internal.engine.db.FabricDatabase
import io.snappydata.benchmark.TPCHColumnPartitionedTable
import io.snappydata.{PlanTest, SnappyFunSuite}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.rdd.ZippedPartitionsPartition
import org.apache.spark.sql.collection.MultiBucketExecutorPartition
import org.apache.spark.sql.execution.columnar.ColumnTableScan

class SingleNodeTest extends SnappyFunSuite with PlanTest with BeforeAndAfterEach {
  var existingSkipSPSCompile = false

  override def beforeAll(): Unit = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.sql.codegen.comments", "true")
    System.setProperty("spark.testing", "true")
    existingSkipSPSCompile = FabricDatabase.SKIP_SPS_PRECOMPILE
    FabricDatabase.SKIP_SPS_PRECOMPILE = true
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.sql.codegen.comments")
    System.clearProperty("spark.testing")
    FabricDatabase.SKIP_SPS_PRECOMPILE = existingSkipSPSCompile
    super.afterAll()
  }

  test("Nodes Pruning") {
    SingleNodeTest.testNodesPruning(snc)
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
    TPCHColumnPartitionedTable.createAndPopulateOrderTable(snc, tpchDataPath,
      true, buckets_Order_Lineitem, null)

    def validateSinglePartition(df: DataFrame, bucketId: Int): Unit = {
      if (true) return // no pruning in this branch yet
      val scanRDD = df.queryExecution.executedPlan.collectFirst {
        case c: ColumnTableScan => c.dataRDD
      }

      val partitions = scanRDD.map(_.partitions).getOrElse(
        throw new AssertionError("Expecting ColumnTable Scan"))
      assert(partitions.length == 1, {
        val sb = new StringBuilder()
        partitions.foreach(p => sb.append(p.index).append(","))
        sb.toString
      })
      val bstr = partitions(0) match {
        case zp: ZippedPartitionsPartition => zp.partitionValues.map {
          case mb: MultiBucketExecutorPartition => mb.bucketsString
        }
        case _ => Seq.empty
      }

      // each BucketExecutor must have only one bucket.
      // there are 2 BucketExecutor entries due to ZipPartion of RowBuffer.
      assert(bstr.forall(_.toInt == bucketId), s"Expected $bucketId, found $bstr")
    }

    var df = snc.sql("select * from orders where o_orderkey = 1 ")
    validateSinglePartition(df, 3)
    assert(df.collect()(0).getInt(0) == 1)

    df = snc.sql("select * from orders where o_orderkey = 32 ")
    validateSinglePartition(df, 0)
    assert(df.collect()(0).getInt(0) == 32)

    df = snc.sql("select * from orders where o_orderkey = 801 ")
    validateSinglePartition(df, 4)
    assert(df.collect()(0).getInt(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = 801 ")
    validateSinglePartition(df, 4)
    assert(df.collect()(0).getInt(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = 1408 ")
    validateSinglePartition(df, 3)
    assert(df.collect()(0).getInt(0) == 1408)

    df = snc.sql("select * from orders where o_orderkey = 1409 ")
    validateSinglePartition(df, 3)
    assert(df.collect()(0).getInt(0) == 1409)

    df = snc.sql("select * from orders where o_orderkey = 1410 ")
    validateSinglePartition(df, 2)
    assert(df.collect()(0).getInt(0) == 1410)

    df = snc.sql("select * from orders where o_orderkey = 1796 ")
    validateSinglePartition(df, 0)
    assert(df.collect()(0).getInt(0) == 1796)

    df = snc.sql("select * from orders where o_orderkey = 801 ")
    validateSinglePartition(df, 4)
    assert(df.collect()(0).getInt(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = '1' ")
    //    validateSinglePartition(df, 3) // complex operator doesn't support pruning.
    assert(df.collect()(0).getInt(0) == 1)

    df = snc.sql("select * from orders where o_orderkey = '32' ")
    //    validateSinglePartition(df, 0) // complex operator doesn't support pruning.
    assert(df.collect()(0).getInt(0) == 32)

    df = snc.sql("select * from orders where o_orderkey = {fn substring('d1xxd2', 2, 1)} ")
    assert(df.collect()(0).getInt(0) == 1)

    df = snc.sql("select * from orders where o_orderkey = substring('acbc801xx', 5, 3) ")
    assert(df.collect()(0).getInt(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = {fn trim(" +
        "substring(' acbc801xx', length(' 12345'), length('801'))) }")
    assert(df.collect()(0).getInt(0) == 801)

    df = snc.sql("select * from orders where o_orderkey = trim(" +
        "substring(' acbc1410xx', length(' 12345'), length('1410'))) ")
    assert(df.collect()(0).getInt(0) == 1410)

    df = snc.sql("select O_ORDERDATE, {fn TIMESTAMPADD(SQL_TSI_DAY," +
        " {fn FLOOR((-1 * {fn DAYOFYEAR(O_ORDERDATE)} - 1))}, O_ORDERDATE)}" +
        " from orders where O_ORDERKEY = 32")
    val r = df.collect()(0)
    assert(r.getDate(0).toString.equals("1995-07-16"))
    assert(r.getDate(1).toString.equals("1994-12-30"))
    // scalastyle:on println
  }
}
