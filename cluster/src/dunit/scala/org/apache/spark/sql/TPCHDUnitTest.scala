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

import java.io.{File, FileOutputStream, PrintStream, PrintWriter}
import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import io.snappydata.benchmark.TPCH_Queries.createQuery
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

  def testSnappy_PrepStatement(): Unit = {
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"testSnappy_PrepStatement: network server started at $serverHostPort")
    // scalastyle:on println

    val snc = SnappyContext(sc)
    logInfo("CREATING TABLES ")
    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)
    runQueriesUsingPrepStatement(conn, snc)
  }

  def runQueriesUsingPrepStatement(conn: Connection, snc: SnappyContext): Unit = {
    val tpchQueries: Array[Int] = (1 to 22).toArray
    val isDynamic: Boolean = false

    // scalastyle:off println
    for (query <- tpchQueries) {
      var prepStatement: PreparedStatement = null
      query match {
        case 1 => {
          println("Executing query#1")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery1)
          val parameters = TPCH_Queries.getQ1Parameter(isDynamic)
          prepStatement.setInt(1, parameters(0).toInt)
        }
        case 2 => {
          println("Executing query#2")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery2ForPrepStatement)
          val parameters = TPCH_Queries.getQ2Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setInt(2, parameters(1).toInt)
          prepStatement.setString(3, "%" + parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case 3 => {
          println("Executing query#3")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery3ForPrepStatement)
          val parameters = TPCH_Queries.getQ3Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setDate(2, Date.valueOf(parameters(1)))
          prepStatement.setDate(3, Date.valueOf(parameters(2)))
        }
        case 4 => {
          println("Executing query#4")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery4ForPrepStatement)
          val parameters = TPCH_Queries.getQ4Parameter(isDynamic)
          prepStatement.setDate(1, Date.valueOf(parameters(0)))
          prepStatement.setDate(2, Date.valueOf(parameters(1)))
        }
        case 5 => {
          println("Executing query#5")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery5ForPrepStatement)
          var parameters = TPCH_Queries.getQ5Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.executeQuery()
        }
        case 6 => {
          println("Executing query#6")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery6ForPrepStatement)
          var parameters = TPCH_Queries.getQ6Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
          prepStatement.setString(5, parameters(4))
        }
        case 7 => {
          println("Executing query#7")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery7ForPrepStatement)
          val parameters = TPCH_Queries.getQ7Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case 8 => {
          println("Executing query#8")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery8ForPrepStatement)
          var parameters = TPCH_Queries.getQ8Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
        }
        case 9 => {
          println("Executing query#9")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery9ForPrepStatement)
          val parameters = TPCH_Queries.getQ9Parameter(isDynamic)
          prepStatement.setString(1, "%" + parameters(0) + "%")
        }
        case 10 => {
          println("Executing query#10")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery10ForPrepStatement)
          val parameters = TPCH_Queries.getQ10Parameter(isDynamic)
          prepStatement.setDate(1, Date.valueOf(parameters(0)))
          prepStatement.setDate(2, Date.valueOf(parameters(1)))
        }
        case 11 => {
          println("Executing query#11")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery11ForPrepStatement)
          val parameters = TPCH_Queries.getQ11Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case 12 => {
          println("Executing query#12")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery12ForPrepStatement)
          val parameters = TPCH_Queries.getQ12Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case 13 => {
          println("Executing query#13")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery13ForPrepStatement)
          val parameters = TPCH_Queries.getQ13Parameter(isDynamic)
          prepStatement.setString(1, "%" + parameters(0) + "%" + parameters(1) + "%")
        }
        case 14 => {
          println("Executing query#14")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery14ForPrepStatement)
          var parameters = TPCH_Queries.getQ14Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case 15 => {
          println("Executing query#15")
          // create a temp view required for Q15
          val queryToBeExecuted =
            createQuery(TPCH_Queries.getQuery15_Temp, TPCH_Queries.getQ15TempParameter(isDynamic))
          val result = snc.sql(queryToBeExecuted)
          result.createGlobalTempView("revenue")
          // prepare Q15
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery15)
        }
        case 16 => {
          println("Executing query#16")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery16ForPrepStatement)
          val parameters = TPCH_Queries.getQ16Parameter(isDynamic)
          prepStatement.setString(1, "Brand#" + parameters(0) + parameters(1))
          prepStatement.setString(2, parameters(2) + "%")
          prepStatement.setInt(3, parameters(3).toInt)
          prepStatement.setInt(4, parameters(4).toInt)
          prepStatement.setInt(5, parameters(5).toInt)
          prepStatement.setInt(6, parameters(6).toInt)
          prepStatement.setInt(7, parameters(7).toInt)
          prepStatement.setInt(8, parameters(8).toInt)
          prepStatement.setInt(9, parameters(9).toInt)
          prepStatement.setInt(10, parameters(10).toInt)
        }
        case 17 => {
          println("Executing query#17")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery17ForPrepStatement)
          val parameters = TPCH_Queries.getQ17Parameter(isDynamic)
          prepStatement.setString(1, "Brand#" + parameters(0) + parameters(1))
          prepStatement.setString(2, parameters(2))
        }
        case 18 => {
          println("Executing query#18")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery18ForPrepStatement)
          val parameters = TPCH_Queries.getQ18Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
        }
        case 19 => {
          println("Executing query#19")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery19ForPrepStatement)
          val parameters = TPCH_Queries.getQ19Parameter(isDynamic)
          prepStatement.setString(1, "Brand#" + parameters(0))
          prepStatement.setInt(2, parameters(1).toInt)
          prepStatement.setInt(3, parameters(2).toInt)
          prepStatement.setString(4, "Brand#" + parameters(3))
          prepStatement.setInt(5, parameters(4).toInt)
          prepStatement.setInt(6, parameters(5).toInt)
          prepStatement.setString(7, "Brand#" + parameters(6))
          prepStatement.setInt(8, parameters(7).toInt)
          prepStatement.setInt(9, parameters(8).toInt)
        }
        case 20 => {
          println("Executing query#20")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery20ForPrepStatement)
          val parameters = TPCH_Queries.getQ20Parameter(isDynamic)
          prepStatement.setString(1, parameters(0) + "%")
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case 21 => {
          println("Executing query#21")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery21ForPrepStatement)
          var parameters = TPCH_Queries.getQ21Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
        }
        case 22 => {
          println("Executing query#22")
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery22ForPrepStatement)
          var parameters = TPCH_Queries.getQ22Parameter(isDynamic)
          prepStatement.setInt(1, parameters(0).toInt)
          prepStatement.setInt(2, parameters(1).toInt)
          prepStatement.setInt(3, parameters(2).toInt)
          prepStatement.setInt(4, parameters(3).toInt)
          prepStatement.setInt(5, parameters(4).toInt)
          prepStatement.setInt(6, parameters(5).toInt)
          prepStatement.setInt(7, parameters(6).toInt)
          prepStatement.setInt(8, parameters(7).toInt)
          prepStatement.setInt(9, parameters(8).toInt)
          prepStatement.setInt(10, parameters(9).toInt)
          prepStatement.setInt(11, parameters(10).toInt)
          prepStatement.setInt(12, parameters(11).toInt)
          prepStatement.setInt(13, parameters(12).toInt)
          prepStatement.setInt(14, parameters(13).toInt)
        }
      }
      if (prepStatement != null) {
        val rs = prepStatement.executeQuery()
        verifyTPCHQueryResult(rs, query)
        rs.close()
        prepStatement.close()
      }

      }
    // scalastyle:on println
    }

  private def verifyTPCHQueryResult(rs: ResultSet, queryNumber: Int): Unit = {
    val rsmd = rs.getMetaData
    val columnsNumber = rsmd.getColumnCount
    var count = 0
    val result = scala.collection.mutable.ArrayBuffer.empty[String]
    val queryResultsFileName = s"JDBCPrepStmtResult_query$queryNumber.txt"
    val writer = new PrintWriter(new File(queryResultsFileName))
    while (rs.next()) {
      count += 1
      var row: String = ""
      for (i <- 1 to columnsNumber) {
        if (i > 1) row += ","
        if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
          // eliminating mismtach due to minor difference in fractional parts
          row = row + rs.getDouble(i).ceil.formatted("%.0f")
        } else {
          row = row + rs.getString(i)
        }
      }
      result += row
      // scalastyle:off println
      writer.println(row)
      // scalastyle:on println
    }
    writer.close()
    // scalastyle:off println
    println(s"Number of rows : $count")
    // scalastyle:on println

    val actualFile = sc.textFile(queryResultsFileName)
    val expectedFile = sc.textFile(getClass.getResource(
      s"/TPCH/RESULT/JDBC/ExpectedJDBCPrepStmtResult_query$queryNumber.txt").getPath)

    val expectedLineSet = expectedFile.collect().toList.sorted
    val actualLineSet = actualFile.collect().toList.sorted

    val expectedNoOfLines = expectedFile.collect().length
    assert(count == expectedNoOfLines, s"For query $queryNumber " +
        s"result count mismatch observed with " +
        s"expected number of rows: ${expectedLineSet.size}" +
        s" and actual number of rows: ${actualLineSet.size}")

    var resultMismatchFound = false
    for ((expectedLine, actualLine) <- expectedLineSet zip actualLineSet) {
      if (!expectedLine.equals(actualLine)) {
        resultMismatchFound = true
        // scalastyle:off println
        println(s"For query $queryNumber result mismatch observed")
        println(s"Expected  : $expectedLine")
        println(s"Found     : $actualLine")
        println(s"-------------------------------------")
        // scalastyle:on println
      }
    }
    assert(!resultMismatchFound, s"For query $queryNumber result mismatch observed")
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
