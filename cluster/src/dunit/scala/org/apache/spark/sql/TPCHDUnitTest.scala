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

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.PreparedStatement

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import io.snappydata.benchmark.snappy.tpch.QueryExecutor
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable, TPCH_Queries}
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.{Logging, SparkContext}

class TPCHDUnitTest(s: String) extends ClusterManagerTestBase(s)
    with Logging {

  override val locatorNetPort: Int = TPCHUtils.locatorNetPort
  val queries = Array("1", "2", "3", "4", "5", "6", "7", "8", "9",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22")
  override val stopNetServersInTearDown = false

  protected val productDir =
    SmartConnectorFunctions.getEnvironmentVariable("SNAPPY_HOME")

  override def beforeClass(): Unit = {
    vm3.invoke(classOf[ClusterManagerTestBase], "startSparkCluster", productDir)
    super.beforeClass()
    startNetworkServersOnAllVMs()
  }

  override def afterClass(): Unit = {
    try {
      vm3.invoke(classOf[ClusterManagerTestBase], "stopSparkCluster", productDir)
      Array(vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
      ClusterManagerTestBase.stopNetworkServers()
    } finally {
      super.afterClass()
    }
  }

  def testSnappy(): Unit = {
    val snc = SnappyContext(sc)

    // create table randomly either using smart connector or
    // from embedded mode
    if ((System.currentTimeMillis() % 2) == 0) {
      logInfo("CREATING TABLE USING SMART CONNECTOR")

      vm3.invoke(classOf[SmartConnectorFunctions],
        "createTablesUsingConnector", locatorNetPort)
    } else {
      logInfo("CREATING TABLE IN EMBEDDED MODE")
      TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    }
    TPCHUtils.queryExecution(snc, isSnappy = true)
    TPCHUtils.validateResult(snc, isSnappy = true)

    vm3.invoke(classOf[SmartConnectorFunctions],
      "queryValidationOnConnector", locatorNetPort)
  }

  /*
    TODO : Kishor
     This test is disabled as of now. For dunit test we are using very small TPCH data i.e.5.5MB.
      With so small data, its quite possible that for some queries result will be same with dynamic parameters
      This needs to make fullproof.
  */
  def _testSnappy_Tokenization(): Unit = {
    val snc = SnappyContext(sc)

    // create table randomly either using smart connector or
    // from embedded mode
    if ((System.currentTimeMillis() % 2) == 0) {
      logInfo("CREATING TABLE USING SMART CONNECTOR")
      vm3.invoke(classOf[SmartConnectorFunctions],
        "createTablesUsingConnector", locatorNetPort)
    } else {
      logInfo("CREATING TABLE IN EMBEDDED MODE")
      TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    }
    TPCHUtils.queryExecution(snc, isSnappy = true, isDynamic = true, fileName = "_FirstRun")
    TPCHUtils.queryExecution(snc, isSnappy = true, isDynamic = true, fileName = "_SecondRun")

    TPCHUtils.validateResult(snc, isSnappy = true, isTokenization = true )

    vm3.invoke(classOf[SmartConnectorFunctions],
      "queryValidationOnConnector", locatorNetPort)
  }

  private def normalizeRow(rows: Array[Row]): Array[String] = {
    val newBuffer: ArrayBuffer[String] = new ArrayBuffer
    val sb = new StringBuilder
    rows.foreach(r => {
      r.toSeq.foreach {
        case d: Double =>
          // round to nearest integer if large enough else
          // round to one decimal digit
          if (math.abs(d) >= 1000) {
            sb.append(math.floor(d + 0.5)).append(',')
          } else {
            sb.append(math.floor(d * 5.0 + 0.25) / 5.0).append(',')
          }
        case bd: java.math.BigDecimal =>
          sb.append(bd.setScale(2, java.math.RoundingMode.HALF_UP)).append(',')
        case v => sb.append(v).append(',')
      }
      newBuffer += sb.toString()
      sb.clear()
    })
    newBuffer.sortWith(_ < _).toArray
  }

  def testTokenization_embedded(): Unit = {
    startNetworkServersOnAllVMs()
    val snc = SnappyContext(sc)

    logInfo("CREATING TABLE IN EMBEDDED MODE")
    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    Thread.sleep(20000)
    runtpchMultipleTimes(snc)
  }

  def testTokenization_split(): Unit = {
    startNetworkServersOnAllVMs()
    val snc = SnappyContext(sc)

    logInfo("CREATING TABLE USING SMART CONNECTOR")
    vm3.invoke(classOf[SmartConnectorFunctions],
      "createTablesUsingConnector", locatorNetPort)
    Thread.sleep(20000)
    runtpchMultipleTimes(snc)
  }

  private def removeLimitClause(query: String): String = {
    val idxstart = query.indexOf("limit ")
    var retquery = query
    if (idxstart > 0) {
      retquery = query.substring(0, idxstart)
    }
    retquery
  }

  private def runtpchMultipleTimes(snc: SnappyContext) = {
    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=1")
    val results: ListBuffer[(String, String, String, Array[String],
        Array[String], Array[String], String)] = new ListBuffer()

    queries.foreach(f = qNum => {
      var queryToBeExecuted1 = removeLimitClause(TPCH_Queries.getQuery(qNum, true, true))
      var queryToBeExecuted2 = removeLimitClause(TPCH_Queries.getQuery(qNum, true, true))
      var queryToBeExecuted3 = removeLimitClause(TPCH_Queries.getQuery(qNum, true, true))
      if (!qNum.equals("15")) {
        val df = snc.sqlUncached(queryToBeExecuted1)
        val res = df.collect()
        val r1 = normalizeRow(res)
        snc.snappySession.clearPlanCache()
        val df2 = snc.sqlUncached(queryToBeExecuted2)
        val res2 = df2.collect()
        val r2 = normalizeRow(res2)
        snc.snappySession.clearPlanCache()
        val df3 = snc.sqlUncached(queryToBeExecuted3)
        val res3 = df3.collect()
        val r3 = normalizeRow(res3)
        snc.snappySession.clearPlanCache()
        results += ((queryToBeExecuted1, queryToBeExecuted2,
            queryToBeExecuted3, r1, r2, r3, qNum))
      }
    })

    var m = SnappySession.getPlanCache
    var cached = 0
    results.foreach(x => {
      val q1 = x._1
      val q2 = x._2
      val q3 = x._3
      val r1 = x._4
      val r2 = x._5
      val r3 = x._6
      val qN = x._7
      val df = snc.sql(q1)
      val res = df.collect()
      val rs1 = normalizeRow(res)
      assert(rs1.sameElements(r1))
      val df2 = snc.sql(q2)
      val res2 = df2.collect()
      val rs2 = normalizeRow(res2)
      assert(rs2.sameElements(r2))
      val df3 = snc.sql(q3)
      val res3 = df3.collect()
      val rs3 = normalizeRow(res3)
      assert(rs3.sameElements(r3))

      m = SnappySession.getPlanCache
      val size = SnappySession.getPlanCache.size()
      snc.snappySession.clearPlanCache()
      if (size == 1) {
        cached = cached + 1
      }

    })
    logInfo(s"Number of queries cached = ${cached}")
    logInfo(s"Size of plan cache = ${SnappySession.getPlanCache.size()}")
    m = SnappySession.getPlanCache
  }


  def _testSpark(): Unit = {
    val snc = new SQLContext(sc)
    TPCHUtils.createAndLoadTables(snc, isSnappy = false)
    TPCHUtils.queryExecution(snc, isSnappy = false)
    TPCHUtils.validateResult(snc, isSnappy = false)
  }

  def testSnap1296_1297(): Unit = {
    val snc = SnappyContext(sc)
    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    val conn = getANetConnection(locatorNetPort)
    val prepStatement = conn.prepareStatement(TPCH_Queries.getQuery10_ForPrepareStatement)
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

  def testRowTablePruning(): Unit = {

    logInfo("Started the Row Table Partition Pruning In SmartConnector")

    vm3.invoke(classOf[SmartConnectorFunctions],
      "verifyRowTablePartitionPruning", locatorNetPort)

    logInfo("Finished the Row Table Partition Pruning In SmartConnector")
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
      s"/TPCH/RESULT/Snappy_10.out").getPath)
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

object TPCHUtils extends Logging {

  val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  val queries = Array("1", "2", "3", "4", "5", "6", "7", "8", "9",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22")

  def createAndLoadTables(snc: SQLContext, isSnappy: Boolean): Unit = {
    val tpchDataPath = getClass.getResource("/TPCH").getPath // "/data/wrk/w/TPCH/1GB"

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
    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath,
      isSnappy, buckets_Order_Lineitem, null)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(snc, tpchDataPath,
      isSnappy, buckets_Order_Lineitem, null)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snc, tpchDataPath,
      isSnappy, buckets_Cust_Part_PartSupp, null)
    TPCHColumnPartitionedTable.createPopulatePartTable(snc, tpchDataPath,
      isSnappy, buckets_Cust_Part_PartSupp, null)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snc, tpchDataPath,
      isSnappy, buckets_Cust_Part_PartSupp, null)
  }

  def validateResult(snc: SQLContext, isSnappy: Boolean, isTokenization: Boolean = false): Unit = {
    val sc: SparkContext = snc.sparkContext

    val fileName = if (!isTokenization) {
      if (isSnappy) "Result_Snappy.out" else "Result_Spark.out"
    } else {
      "Result_Snappy_Tokenization.out"
    }

    val resultsLogFileStream: FileOutputStream = new FileOutputStream(new File(fileName))
    val resultsLogStream: PrintStream = new PrintStream(resultsLogFileStream)

    // scalastyle:off
    for (query <- queries) {
      println(s"For Query $query")

      if (!isTokenization) {
        val expectedFile = sc.textFile(getClass.getResource(
          s"/TPCH/RESULT/Snappy_$query.out").getPath)

        //val queryFileName = if (isSnappy) s"1_Snappy_$query.out" else s"1_Spark_$query.out"
        val queryResultsFileName = if (isSnappy) s"1_Snappy_Q${query}_Results.out" else s"1_Spark_Q${query}_Results.out"
        val actualFile = sc.textFile(queryResultsFileName)

        val expectedLineSet = expectedFile.collect().toList.sorted
        val actualLineSet = actualFile.collect().toList.sorted

        if (!actualLineSet.equals(expectedLineSet)) {
          if (!(expectedLineSet.size == actualLineSet.size)) {
            resultsLogStream.println(s"For $query " +
                s"result count mismatched observed with " +
                s"expected ${expectedLineSet.size} and actual ${actualLineSet.size}")
          } else {
            for ((expectedLine, actualLine) <- expectedLineSet zip actualLineSet) {
              if (!expectedLine.equals(actualLine)) {
                resultsLogStream.println(s"For $query result mismatched observed")
                resultsLogStream.println(s"Expected  : $expectedLine")
                resultsLogStream.println(s"Found     : $actualLine")
                resultsLogStream.println(s"-------------------------------------")
              }
            }
          }
        }
      } else {
        val firstRunFileName = s"Snappy_${query}_FirstRun.out"
        val firstRunFile = sc.textFile(firstRunFileName)

        val secondRunFileName = s"Snappy_${query}_SecondRun.out"
        val secondRunFile = sc.textFile(secondRunFileName)

        val expectedLineSet = firstRunFile.collect().toList.sorted
        val actualLineSet = secondRunFile.collect().toList.sorted

        if (actualLineSet.equals(expectedLineSet)) {
          resultsLogStream.println(s"For $query result matched observed")
          resultsLogStream.println(s"-------------------------------------")
        }
      }
    }
    // scalastyle:on
    resultsLogStream.close()
    resultsLogFileStream.close()

    val resultOutputFile = sc.textFile(fileName)

    if(!isTokenization) {
      assert(resultOutputFile.count() == 0,
        s"Query result mismatch Observed. Look at Result_Snappy.out for detailed failure")
      if (resultOutputFile.count() != 0) {
        logWarning(
          s"QUERY RESULT MISMATCH OBSERVED. Look at Result_Snappy.out for detailed failure")
      }
    } else {
      assert(resultOutputFile.count() == 0,
        s"Query result match Observed. Look at Result_Snappy_Tokenization.out for detailed failure")
      if (resultOutputFile.count() != 0) {
        logWarning(
          s"QUERY RESULT MATCH OBSERVED. Look at Result_Snappy_Tokenization.out for detailed" +
              s" failure")
      }
    }
  }

  def queryExecution(snc: SQLContext, isSnappy: Boolean, isDynamic: Boolean = false,
      warmup: Int = 0, runsForAverage: Int = 1, isResultCollection: Boolean = true,
      fileName: String = ""): Unit = {
    snc.sql(s"set spark.sql.crossJoin.enabled = true")

    queries.foreach(query => QueryExecutor.execute(query, snc, isResultCollection,
      isSnappy, isDynamic = isDynamic, warmup = warmup, runsForAverage = runsForAverage,
      avgTimePrintStream = System.out))
  }
}
