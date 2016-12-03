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

import java.io.{File, FileOutputStream, PrintStream}

import io.snappydata.benchmark.snappy.TPCH_Snappy
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}
import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.SparkContext

class TPCHDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testSnappy(): Unit = {
    val snc = SnappyContext(sc)
    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    TPCHUtils.queryExecution(snc, isSnappy = true)
    TPCHUtils.validateResult(snc, isSnappy = true)
  }

  def _testSpark(): Unit = {
    val snc = SnappyContext(sc)
    TPCHUtils.createAndLoadTables(snc, isSnappy = false)
    TPCHUtils.queryExecution(snc, isSnappy = false)
    TPCHUtils.validateResult(snc, isSnappy = false)
  }

}

object TPCHUtils {

  val queries = Array("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9",
    "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19",
    "q20", "q21", "q22")

  def createAndLoadTables(snc: SnappyContext, isSnappy: Boolean): Unit = {
    val tpchDataPath = getClass.getResource("/TPCH").getPath

    val usingOptionString =
      s"""
           USING row
           OPTIONS ()"""

    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, snc,
      tpchDataPath, isSnappy, null)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, snc,
      tpchDataPath, isSnappy, null)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, snc,
      tpchDataPath, isSnappy, null)

    val buckets_Order_Lineitem = "5"
    val buckets_Cust_Part_PartSupp = "5"
    TPCHColumnPartitionedTable.createAndPopulateOrderTable(snc, tpchDataPath,
      isSnappy, buckets_Order_Lineitem, null)
    TPCHColumnPartitionedTable.createAndPopulateLineItemTable(snc, tpchDataPath,
      isSnappy, buckets_Order_Lineitem, null)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snc, tpchDataPath,
      isSnappy, buckets_Cust_Part_PartSupp, null)
    TPCHColumnPartitionedTable.createPopulatePartTable(snc, tpchDataPath,
      isSnappy, buckets_Cust_Part_PartSupp, null)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snc, tpchDataPath,
      isSnappy, buckets_Cust_Part_PartSupp, null)
  }

  def validateResult(snc: SnappyContext, isSnappy: Boolean): Unit = {
    val sc: SparkContext = snc.sparkContext

    val fineName = if (isSnappy) "Result_Snappy.out" else "Result_Spark.out"

    val resultFileStream: FileOutputStream = new FileOutputStream(new File(fineName))
    val resultOutputStream: PrintStream = new PrintStream(resultFileStream)

    // scalastyle:off
    for (query <- queries) {
      println(s"For Query $query")

      val expectedFile = sc.textFile(getClass.getResource(
        s"/TPCH/RESULT/Snappy_$query.out").getPath)

      val queryFileName = if (isSnappy) s"Snappy_$query.out" else s"Spark_$query.out"
      val actualFile = sc.textFile(queryFileName)

      val expectedLineSet = expectedFile.collect().toList.sorted
      val actualLineSet = actualFile.collect().toList.sorted

      if (!actualLineSet.equals(expectedLineSet)) {
        if (!(expectedLineSet.size == actualLineSet.size)) {
          resultOutputStream.println(s"For $query " +
              s"result count mismatched observed with " +
              s"expected ${expectedLineSet.size} and actual ${actualLineSet.size}")
        } else {
          for ((expectedLine, actualLine) <- expectedLineSet zip actualLineSet) {
            if (!expectedLine.equals(actualLine)) {
              resultOutputStream.println(s"For $query result mismatched observed")
              resultOutputStream.println(s"Excpected : $expectedLine")
              resultOutputStream.println(s"Found     : $actualLine")
              resultOutputStream.println(s"-------------------------------------")
            }
          }
        }
      }
    }
    // scalastyle:on
    resultOutputStream.close()
    resultFileStream.close()

    val resultOutputFile = sc.textFile(fineName)
    assert(resultOutputFile.count() == 0,
      s"Query mismatch Observed. Look at Result_Snappy.out for detailed failure")
    if (resultOutputFile.count() != 0) {
      ClusterManagerTestBase.logger.warn(
        s"QUERY MISMATCH OBSERVED. Look at Result_Snappy.out for detailed failure")
    }
  }

  def queryExecution(snc: SnappyContext, isSnappy: Boolean): Unit = {
    snc.sql(s"set spark.sql.shuffle.partitions= 4")
    snc.sql(s"set spark.sql.crossJoin.enabled = true")

    queries.foreach(query => TPCH_Snappy.execute(query, snc,
      isResultCollection = true, isSnappy = isSnappy, avgPrintStream = System.out))
  }
}