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
import java.sql.PreparedStatement

import io.snappydata.benchmark.snappy.TPCH_Snappy
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.sql.TPCHUtils._

class TPCHDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  bootProps.setProperty(io.snappydata.Property.CachedBatchSize.name, "10000")
  
  val queries = Array("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9",
    "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19",
    "q20", "q21", "q22")

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

  def testSnap1296_1297(): Unit = {
    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    val conn = getANetConnection(netPort1)
    val prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery10)
    verifyResultSnap1296_1297(prepStatement)
    prepStatement.close()

// TODO: Enable the test below after fixing SNAP-1323
//    val prepStatement2 = conn.prepareStatement(getTPCHQuery10Parameterized)
//    val pmd = prepStatement2.getParameterMetaData
//    println("pmd = " + pmd  + " pmd.getParameterCount =" + pmd.getParameterCount)
//    prepStatement2.setString(1, "1993-10-01")
//    prepStatement2.setString(2, "1993-10-01")
//
//    verifyResultSnap1296_1297(prepStatement2)
//    prepStatement2.close()

  }

  private def verifyResultSnap1296_1297(prepStatement: PreparedStatement): Unit = {
    val rs = prepStatement.executeQuery
    val rsmd = rs.getMetaData()
    val columnsNumber = rsmd.getColumnCount()
    var count = 0
    val result = scala.collection.mutable.ArrayBuffer.empty[String]
    while (rs.next()) {
      count += 1
      var row: String = ""
      for (i <- 1 to columnsNumber) {
        if (i > 1) row += ","
        row = row + rs.getString(i)
      }
      result += row
    }
    println(s"Number of rows : $count")

    val expectedFile = sc.textFile(getClass.getResource(
      s"/TPCH/RESULT/Snappy_q10.out").getPath)
    val expectedNoOfLines = expectedFile.collect().size
    assert(count == expectedNoOfLines)
  }

  private def getTPCHQuery10Parameterized: String = {
    "select" +
        "         C_CUSTKEY," +
        "         C_NAME," +
        "         sum(l_extendedprice * (1 - l_discount)) as revenue," +
        "         C_ACCTBAL," +
        "         n_name," +
        "         C_ADDRESS," +
        "         C_PHONE," +
        "         C_COMMENT" +
        " from" +
        "         ORDERS," +
        "         LINEITEM," +
        "         CUSTOMER," +
        "         NATION" +
        " where" +
        "         C_CUSTKEY = o_custkey" +
        "         and l_orderkey = o_orderkey" +
        "         and o_orderdate >= ?" +
        "         and o_orderdate < add_months(?, 3)" +
        "         and l_returnflag = 'R'" +
        "         and C_NATIONKEY = n_nationkey" +
        " group by" +
        "         C_CUSTKEY," +
        "         C_NAME," +
        "         C_ACCTBAL," +
        "         C_PHONE," +
        "         n_name," +
        "         C_ADDRESS," +
        "         C_COMMENT" +
        " order by" +
        "         revenue desc" +
        " limit 20"
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

  def queryExecution(snc: SnappyContext, isSnappy: Boolean, warmup: Int = 0,
      runsForAverage: Int = 1, isResultCollection: Boolean = true): Unit = {
    snc.sql(s"set spark.sql.crossJoin.enabled = true")

    queries.foreach(query => TPCH_Snappy.execute(query, snc,
      isResultCollection, isSnappy, warmup = warmup,
      runsForAverage = runsForAverage, avgPrintStream = System.out))
  }
}
