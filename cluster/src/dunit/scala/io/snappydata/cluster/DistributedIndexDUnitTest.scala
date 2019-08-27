/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.cluster

import java.sql.{Connection, DriverManager}

import scala.collection.mutable.ListBuffer

import com.gemstone.gemfire.cache.CacheException
import com.pivotal.gemfirexd.internal.engine.access.index.{OpenMemIndex, SortedMap2IndexScanController}
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.{GemFireXDQueryObserver, GemFireXDQueryObserverAdapter, GemFireXDQueryObserverHolder}
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.Conglomerate
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode
import io.snappydata.benchmark.TPCHColumnPartitionedTable
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.store.CreateIndexTest
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.sql.collection.Utils

/**
 * Tests various distributed index related tests.
 */
class DistributedIndexDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  // SNAP-1800 Disabled all tests in this dunit
  private val disabled = true

  val tablesToDrop = new ListBuffer[String]
  val indexesToDrop = new ListBuffer[String]
  override def tearDown2(): Unit = {
    if (disabled) {
      super.tearDown2()
      return
    }
    try {
      val snContext = SnappyContext(sc)
      if (snContext != null) {
        snContext.setConf(io.snappydata.Property.EnableExperimentalFeatures.name,
          io.snappydata.Property.EnableExperimentalFeatures.configEntry.defaultValueString)
        snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get.toString)
        indexesToDrop.reverse.foreach(i => snContext.sql(s"DROP INDEX if exists $i "))
        tablesToDrop.reverse.foreach(t => snContext.sql(s"DROP TABLE if exists $t "))
        indexesToDrop.clear()
        tablesToDrop.clear()
      }
    } finally {
      super.tearDown2()
    }
  }

  def createBaseTable(snContext: SnappyContext, tableName: String): Unit = {
    val props = Map(
      "PARTITION_BY" -> "col1")
    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111, "aaa", "hello"),
      Seq(222, "bbb", "halo"),
      Seq(333, "aaa", "hello"),
      Seq(444, "bbb", "halo"),
      Seq(555, "ccc", "halo"),
      Seq(666, "ccc", "halo")
    )

    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    tablesToDrop += tableName
  }

  def testPartitionedSingleColumnTable(): Unit = {
    if (disabled) return

    val tableName = "tabOne"

    val snContext = SnappyContext(sc)
    snContext.setConf(io.snappydata.Property.EnableExperimentalFeatures.configEntry.key, "true")
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    createBaseTable(snContext, tableName)
    getLogWriter.info("Creating indexes")
    val indexOne = s"${tableName}_IdxOne"
    val indexTwo = s"${tableName}_IdxTwo"
    val indexThree = s"${tableName}_IdxThree"
//    snContext.sql(s"create index $indexOne on $tableName (COL1)")
//    indexesToDrop += indexOne
    snContext.sql(s"create index $indexTwo on $tableName (COL2, COL3)")
    indexesToDrop += indexTwo
    snContext.sql(s"create index $indexThree on $tableName (COL1, COL3)")
    indexesToDrop += indexThree

    val executeQ = CreateIndexTest.QueryExecutor(snContext)
//    executeQ(s"select * from $tableName where col1 = 111") {
//      CreateIndexTest.validateIndex(Seq(indexOne))(_)
//    }

//    executeQ(s"select * from $tableName where col2 = 'aaa' ") {
//      CreateIndexTest.validateIndex(Nil, tableName)(_)
//    }

    executeQ(s"select * from $tableName where col2 = 'bbb' and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexTwo))(_)
    }

    executeQ(s"select * from $tableName where col1 = 111 and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexThree))(_)
    }
  }

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testCreateDropColumnTable(): Unit = {
    if (disabled) return

    val tableName = "tabOne"
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    val snContext = SnappyContext(sc)
    snContext.setConf(io.snappydata.Property.EnableExperimentalFeatures.configEntry.key, "true")
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    createBaseTable(snContext, tableName)
    getLogWriter.info("Creating indexes")
    val indexOne = s"${tableName}_IdxOne"
    val indexTwo = s"${tableName}_IdxTwo"
    val indexThree = s"${tableName}_IdxThree"
    //    snContext.sql(s"create index $indexOne on $tableName (COL1)")
    //    indexesToDrop += indexOne
    val s1 = conn.createStatement()
    s1.execute(s"create index $indexTwo on $tableName (COL2, COL3)")
    indexesToDrop += indexTwo
    val s2 = conn.createStatement()
    s2.execute(s"create index $indexThree on $tableName (COL1, COL3)")
    indexesToDrop += indexThree

    val executeQ = CreateIndexTest.QueryExecutor(snContext)
    //    executeQ(s"select * from $tableName where col1 = 111") {
    //      CreateIndexTest.validateIndex(Seq(indexOne))(_)
    //    }

    //    executeQ(s"select * from $tableName where col2 = 'aaa' ") {
    //      CreateIndexTest.validateIndex(Nil, tableName)(_)
    //    }

    System.setProperty("LOG-NOW", "xxx")
    getLogWriter.info("SB: About to execute queries")
    executeQ(s"select * from $tableName where col2 = 'bbb' and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexTwo))(_)
    }

    executeQ(s"select * from $tableName where col1 = 111 and col3 = 'halo' ") {
      CreateIndexTest.validateIndex(Seq(indexThree))(_)
    }

    val d1 = conn.createStatement()
    d1.execute(s"drop index $indexTwo")
    val d2 = conn.createStatement()
    d2.execute(s"drop index $indexThree")

    getLogWriter.info("SB: Done executing the queries")
    System.clearProperty("LOG-NOW")
  }

  // Part of fix to SNAP-1461
  // This is being commented out. This is because now even the replicated
  // table queries which are not pkbased or convertible to getAll are being routed
  // and the test below asserts on an index being used assuming store execution.
  def testCreateDropRowTable(): Unit = {
    if (disabled) return

    val tableName = "tabTwo"
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    val snContext = SnappyContext(sc)
    snContext.setConf(io.snappydata.Property.EnableExperimentalFeatures.configEntry.key, "true")
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

    val s = conn.createStatement()
    s.executeUpdate(s"create table $tableName (COL1 Int, COL2 Int, COL3 Int) using row")
    s.executeUpdate(s"insert into $tableName values (111, 11, 81)")
    s.executeUpdate(s"insert into $tableName values (222, 22, 91)")
    s.executeUpdate(s"insert into $tableName values (333, 11, 81)")
    s.executeUpdate(s"insert into $tableName values (444, 22, 91)")
    s.executeUpdate(s"insert into $tableName values (555, 33, 91)")
    s.executeUpdate(s"insert into $tableName values (666, 33, 91)")

    getLogWriter.info("Creating indexes")
    val indexOne = s"${tableName}_IdxOne"
    val indexTwo = s"${tableName}_IdxTwo"
    val indexThree = s"${tableName}_IdxThree"
    //    snContext.sql(s"create index $indexOne on $tableName (COL1)")
    //    indexesToDrop += indexOne
    s.executeUpdate(s"create index $indexTwo on $tableName (COL2, COL3)")
    indexesToDrop += indexTwo
    s.executeUpdate(s"create index $indexThree on $tableName (COL1, COL3)")
    indexesToDrop += indexThree

    // val executeQ = CreateIndexTest.QueryExecutor(snContext)
    //    executeQ(s"select * from $tableName where col1 = 111") {
    //      CreateIndexTest.validateIndex(Seq(indexOne))(_)
    //    }

    //    executeQ(s"select * from $tableName where col2 = 'aaa' ") {
    //      CreateIndexTest.validateIndex(Nil, tableName)(_)
    //    }

    System.setProperty("LOG-NOW", "xxx")
    getLogWriter.info("SB: About to execute queries")

    val query1 = s"select * from $tableName where col2 = 22 and col3 = 91"
    val query2 = s"select * from $tableName where col1 =111 and col3 = 81"
    setIndexObserver(s"$indexTwo", s"$query1", s"$indexThree", s"$query2")

    val rs1 = s.executeQuery(s"$query1")
    while(rs1.next()) {
      getLogWriter.info("q1= " + rs1.getInt(1))
    }

    val rs2 = s.executeQuery(s"$query2")
    while(rs2.next()) {
      getLogWriter.info("q2= " + rs2.getInt(1))
    }

    unsetObserver()
    s.execute(s"drop index $indexTwo")
    s.execute(s"drop index $indexThree")

    getLogWriter.info("SB: Done executing the queries")
    System.clearProperty("LOG-NOW")
  }

  def setIndexObserver(indexTwo: String, queryTwo: String, indexThree: String, queryThree: String):
  Unit = {
    val hook = new SerializableRunnable {
      override def run() {
        val executionEngineObserver: GemFireXDQueryObserver = new GemFireXDQueryObserverAdapter() {
          var indexTwoPicked: Boolean = false
          var caseOfIndexTwo: Boolean = false
          var indexThreePicked: Boolean = false
          var caseOfIndexThree: Boolean = false

          override def afterQueryParsing(query: String, qt: StatementNode, lcc:
          LanguageConnectionContext): Unit = {
            if (query != null) {
              if (!caseOfIndexTwo) {
                caseOfIndexTwo = query.equalsIgnoreCase(queryTwo)
              }

              if (!caseOfIndexThree) {
                caseOfIndexThree = query.equalsIgnoreCase(queryThree)
              }
            }
          }

          override def overrideDerbyOptimizerIndexUsageCostForHash1IndexScan(memIndex: OpenMemIndex,
              optimzerEvalutatedCost: Double): Double = Double.MaxValue

          override def overrideDerbyOptimizerCostForMemHeapScan(gfContainer: GemFireContainer,
              optimzerEvalutatedCost: Double): Double = Double.MaxValue

          override def overrideDerbyOptimizerIndexUsageCostForSortedIndexScan(memIndex:
          OpenMemIndex, optimzerEvalutatedCost: Double): Double = 1

          override def scanControllerOpened(sc: AnyRef, conglom: Conglomerate) {
            if (caseOfIndexTwo && !indexTwoPicked) {
              indexTwoPicked = sc match {
                case smisc: SortedMap2IndexScanController =>
                  smisc.getQualifiedIndexName.split(":base-table:")(0).equalsIgnoreCase(s"APP" +
                      s".$indexTwo")
                case _ => false
              }
            }
            if (caseOfIndexThree && !indexThreePicked) {
              indexThreePicked = sc match {
                case smisc: SortedMap2IndexScanController =>
                  smisc.getQualifiedIndexName.split(":base-table:")(0).equalsIgnoreCase(s"APP" +
                      s".$indexThree")
                case _ => false
              }
            }
          }

          override def close(): Unit = {
            if (caseOfIndexTwo) {
              assert(indexTwoPicked)
            }

            if (caseOfIndexThree) {
              assert(indexThreePicked)
            }
          }
        }

        GemFireXDQueryObserverHolder.setInstance(executionEngineObserver)
      }
    }

    hook.run()
    vm0.invoke(hook)
    vm1.invoke(hook)
    vm2.invoke(hook)
    vm3.invoke(hook)
  }

  def unsetObserver(): Unit = {
    val hook = new SerializableRunnable {
      override def run() {
        try {
          GemFireXDQueryObserverHolder.clearInstance()
        }
        catch {
          case e: Exception => throw new CacheException(e){}
        }
      }
    }

    hook.run()
    vm0.invoke(hook)
    vm1.invoke(hook)
    vm2.invoke(hook)
    vm3.invoke(hook)
  }

}
