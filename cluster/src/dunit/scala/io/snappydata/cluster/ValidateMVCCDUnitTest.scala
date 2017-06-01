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

package io.snappydata.cluster

import java.sql.DriverManager

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl.RvvSnapshotTestHook
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.{FabricService, TestUtil}
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, SerializableRunnable, VM}
import io.snappydata.{Locator, ServiceManager}
import org.slf4j.LoggerFactory

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils

class ValidateMVCCDUnitTest(val s: String) extends ClusterManagerTestBase(s) with Logging {

  // set default batch size for this test
  bootProps.setProperty(io.snappydata.Property.ColumnBatchSize.name, "4")
  var errorInThread: Throwable = null

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  override def beforeClass(): Unit = {
    val testName = getName
    val testClass = getClass
    // bootProps.setProperty(Attribute.SYS_PERSISTENT_DIR, s)
    TestUtil.currentTest = testName
    TestUtil.currentTestClass = getTestClass
    TestUtil.skipDefaultPartitioned = true
    TestUtil.doCommonSetup(bootProps)
    GemFireXDUtils.IS_TEST_MODE = true

    getLogWriter.info("\n\n\n  STARTING TEST " + testClass.getName + '.' +
        testName + "\n\n")

    val locNetPort = locatorNetPort
    val locNetProps = locatorNetProps
    val locPort = ClusterManagerTestBase.locPort
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        val loc: Locator = ServiceManager.getLocatorInstance

        if (loc.status != FabricService.State.RUNNING) {
          loc.start("localhost", locPort, locNetProps)
        }
        if (locNetPort > 0) {
          loc.startNetworkServer("localhost", locNetPort, locNetProps)
        }
        assert(loc.status == FabricService.State.RUNNING)

        val logger = LoggerFactory.getLogger(getClass)
        logger.info("\n\n\n  STARTING TESTS IN " + getClass.getName + "\n\n")
      }
    })
    val nodeProps = bootProps
    val startNode = new SerializableRunnable() {
      override def run(): Unit = {
        val node = ServiceManager.currentFabricServiceInstance
        if (node == null || node.status != FabricService.State.RUNNING) {
          ClusterManagerTestBase.startSnappyServer(locPort, nodeProps)
        }
        assert(ServiceManager.currentFabricServiceInstance.status ==
            FabricService.State.RUNNING)

        val logger = LoggerFactory.getLogger(getClass)
        logger.info("\n\n\n  STARTING TESTS IN " + getClass.getName + "\n\n")
      }
    }

    vm0.invoke(startNode)
    // vm1.invoke(startNode)
    // vm2.invoke(startNode)

    // start lead node in this VM
    val sc = SnappyContext.globalSparkContext
    if (sc == null || sc.isStopped) {
      ClusterManagerTestBase.startSnappyLead(locPort, bootProps)
    }
    assert(ServiceManager.currentFabricServiceInstance.status ==
        FabricService.State.RUNNING)
  }

  override def tearDownAfter(): Unit = {
    invokeMethodInVm(vm0, classOf[ValidateMVCCDUnitTest], "clearTestHook", 0)
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def testMVCCForColumnTable(): Unit = {
    errorInThread = null
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val snc = SnappyContext(sc)
    val tableName: String = "TESTTABLE"
    // snc.sql("set spark.sql.inMemoryColumnarStorage.batchSize = 5")

    snc.sql(s"create table $tableName(col1 integer, col2 String, col3 integer) using column " +
        s"OPTIONS (PARTITION_BY 'col1', buckets '1',MAXPARTSIZE '200'," +
        s"COLUMN_MAX_DELTA_ROWS '10',COLUMN_BATCH_SIZE " +
        s"'5000')")

    // Invoking validate result in each VM as a separate thread inorder to resume the code for
    // insertion of records
    invokeMethodInVm(vm0, classOf[ValidateMVCCDUnitTest], "validateResults", netPort1)


    for (i <- 1 to 10) {
      snc.sql(s"insert into $tableName values($i,'${i + 1}',${i + 2})")
    }

    val cnt = snc.sql(s"select * from $tableName").count()

    assert(cnt == 10, s"Expected row count is 10 while actual row count is $cnt")
    snc.sql(s"drop table $tableName")

    if (errorInThread != null) {

      throw errorInThread
    }

    // scalastyle:off
    println("Successful")
    // scalastyle:on

  }

  def invokeMethodInVm(vM: VM,
      classType: Class[ValidateMVCCDUnitTest],
      methodName: String, netPort1: Int): Unit = {

    new Thread {

      override def run: Unit = {
        try {
          vM.invoke(classType, methodName, netPort1)
        } catch {
          case e: Throwable =>
            errorInThread = e
        }
      }
    }.start()

  }


  def testMVCCForColumnTableWithRollback(): Unit = {
    errorInThread = null
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val snc = SnappyContext(sc)
    val tableName: String = "TESTTABLE"
    // snc.sql("set spark.sql.inMemoryColumnarStorage.batchSize = 5")

    snc.sql(s"create table $tableName(col1 integer, col2 String, col3 integer) using column " +
        s"OPTIONS (PARTITION_BY 'col1'," +
        s" buckets '1',MAXPARTSIZE '200',COLUMN_MAX_DELTA_ROWS '10',COLUMN_BATCH_SIZE " +
        s"'5000')")

    // Invoking validate result in each VM as a separate thread inorder to resume the code for
    // insertion of records
    invokeMethodInVm(vm0, classOf[ValidateMVCCDUnitTest], "validateResultsWithRollback", netPort1)

    /*
        val rdd1 = sc.parallelize(
          (1 to 10).map(i => Data(i, i.toString, Decimal(i.toString + '.' + i))))

        val dataDF1 = snc.createDataFrame(rdd1)
        //Write 5 records as batch size is set to 2 it will trigger the cachebatch creation
        dataDF1.write.insertInto(tableName) */
    var cnt = snc.sql(s"select * from $tableName").count()

    assert(cnt == 0, s"Expected row count is 0 while actual row count is $cnt")

    try {
      for (i <- 1 to 10) {
        snc.sql(s"insert into $tableName values($i, '${i + 1}', ${i + 2})")
      }
    } catch {
      case rex: Throwable => // As expected
    }

    cnt = snc.sql(s"select * from $tableName").count()

    assert(cnt == 10, s"Expected row count is 10 while actual row count is $cnt")
    snc.sql(s"drop table $tableName")
    if (errorInThread != null) {
      throw errorInThread
    }
    // scalastyle:off
    println("Successful")
    // scalastyle:on

  }

}

object ValidateMVCCDUnitTest {

  class MyTestHook extends RvvSnapshotTestHook {
    val lockForTest: AnyRef = new AnyRef
    val operationLock: AnyRef = new AnyRef

    override def notifyOperationLock(): Unit = {
      operationLock.synchronized {
        operationLock.notify()
      }
    }

    override def notifyTestLock(): Unit = {
      lockForTest.synchronized {
        lockForTest.notify()
      }
    }

    override def waitOnTestLock(): Unit = {
      lockForTest.synchronized {
        lockForTest.wait(60000)
      }
    }

    override def waitOnOperationLock(): Unit = {
      operationLock.synchronized {
        operationLock.wait(60000)
      }
    }
  }

  def validateResults(netPort: Int): Unit = {

    val cache = GemFireCacheImpl.getInstance()
    if (null != cache) {
      cache.setRvvSnapshotTestHook(new MyTestHook)
      cache.waitOnRvvTestHook()

    } else {
      return;
    }
    // scalastyle:off
    println("Got notification from test hook")
    // scalastyle:on
    if (null == cache) {
      return;
    }
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    var url: String = null

    url = "jdbc:snappydata://localhost:" + netPort + "/"

    val tableName: String = "APP.TESTTABLE"
    val conn = DriverManager.getConnection(url)


    val s = conn.createStatement()
    s.execute(s"select * from $tableName")
    var cnt = 0
    val rs = s.getResultSet
    while (rs.next) {
      cnt = cnt + 1
      // scalastyle:off
      println("Resultset:  " + rs.getInt(1))
      // scalastyle:on
    }

    // scalastyle:off
    println("Row count before creating the cachebatch: " + cnt)
    // scalastyle:on
    assert(cnt == 10, s"Expected row count is 10 while actual row count is $cnt")


    var cnt1 = 0;
    s.execute(s"select * from $tableName -- GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs1 = s.getResultSet
    while (rs1.next) {
      cnt1 = cnt1 + 1
      // scalastyle:off
      println("Resultset from row buffer:  " + rs1.getInt(1))
      // scalastyle:on
    }
    // scalastyle:off
    println("Row count before creating the cachebatch in row buffer: " + cnt1)
    // scalastyle:on
    assert(cnt1 == 10, s"Expected row count is 10 while actual row count is $cnt1")

    var cnt2 = 0;
    s.execute(s"select * from SNAPPYSYS_INTERNAL.APP__TESTTABLE_COLUMN_STORE_ -- " +
        s"GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs2 = s.getResultSet
    while (rs2.next) {
      cnt2 = cnt2 + 1
    }
    // scalastyle:off
    println("Row count before creating the cachebatch in column store: " + cnt2)
    // scalastyle:on
    assert(cnt2 == 0, s"Expected row count is 0 while actual row count is $cnt2")

    cache.notifyRvvSnapshotTestHook()
    cache.waitOnRvvTestHook()


    var cnt3 = 0;
    s.execute(s"select * from $tableName -- GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs3 = s.getResultSet
    while (rs3.next) {
      cnt3 = cnt3 + 1
    }

    // scalastyle:off
    println("Row count in row buffer after destroy all entries from row buffer but no commit  : " + cnt3)
    // scalastyle:on
    assert(cnt3 == 10, s"Expected row count is 10 while actual row count is $cnt3")

    cache.notifyRvvSnapshotTestHook()


    cache.waitOnRvvTestHook()
    cache.setRvvSnapshotTestHook(null)


    var cnt4 = 0;
    s.execute(s"select * from SNAPPYSYS_INTERNAL.APP__TESTTABLE_COLUMN_STORE_ -- " +
        s"GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs4 = s.getResultSet
    while (rs4.next) {
      cnt4 = cnt4 + 1
    }
    // scalastyle:off
    println("Row count in column store after destroy all entries from row buffer " +
        "and reinitialize snapshot   : " + cnt4)
    // scalastyle:on
    // The number of entries in column store is 4
    // as after columnwise storage 3 rows will be created one for each
    // column and 4th row is for stats
    assert(cnt4 == 4, s"Expected Count is 4 but actual count is $cnt4")


    var cnt5 = 0;
    s.execute(s"select * from $tableName -- GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs5 = s.getResultSet
    while (rs5.next) {
      cnt5 = cnt5 + 1
    }

    // scalastyle:off
    println("Row count in row buffer after destroy all entries from row buffer " +
        "and reinitialize snapshot   : " + cnt5)
    // scalastyle:on
    assert(cnt5 == 0, s"Expected row count is 0 while actual row count is $cnt5")

    var cnt6 = 0;
    s.execute(s"select * from $tableName")
    val rs6 = s.getResultSet
    while (rs6.next) {
      cnt6 = cnt6 + 1
    }
    // scalastyle:off
    println("Row count in column table : " + cnt6)
    // scalastyle:on
    assert(cnt6 == 10, s"Expected row count is 10 while actual row count is $cnt6")

  }


  def validateResultsWithRollback(netPort: Int): Unit = {

    val cache = GemFireCacheImpl.getInstance()
    if (null != cache) {
      cache.setRvvSnapshotTestHook(new MyTestHook)
      cache.getCacheTransactionManager.testRollBack = true;
      cache.waitOnRvvTestHook()

    } else {
      return;
    }

    if (null == cache) {
      return;
    }
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    var url: String = null

    url = "jdbc:snappydata://localhost:" + netPort + "/"

    val tableName: String = "APP.TESTTABLE"
    val conn = DriverManager.getConnection(url)


    val s = conn.createStatement()
    s.execute(s"select * from $tableName")
    var cnt = 0
    val rs = s.getResultSet
    while (rs.next) {
      cnt = cnt + 1
      // scalastyle:off
      println("Resultset:  " + rs.getInt(1))
      // scalastyle:on
    }

    // scalastyle:off
    println("Row count before creating the cachebatch: " + cnt)
    // scalastyle:on
    assert(cnt == 10, s"Expected row count is 10 while actual row count is $cnt")


    var cnt1 = 0;
    s.execute(s"select * from $tableName -- GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs1 = s.getResultSet
    while (rs1.next) {
      cnt1 = cnt1 + 1
      // scalastyle:off
      println("Resultset from row buffer:  " + rs1.getInt(1))
      // scalastyle:on
    }
    // scalastyle:off
    println("Row count before creating the cachebatch in row buffer: " + cnt1)
    assert(cnt1 == 10, s"Expected row count is 10 while actual row count is $cnt1")

    var cnt2 = 0;
    s.execute(s"select * from SNAPPYSYS_INTERNAL.APP__TESTTABLE_COLUMN_STORE_ -- " +
        s"GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs2 = s.getResultSet
    while (rs2.next) {
      cnt2 = cnt2 + 1
    }
    // scalastyle:off
    println("Row count before creating the cachebatch in column store: " + cnt2)
    // scalastyle:on
    assert(cnt2 == 0, s"Expected row count is 0 while actual row count is $cnt2")

    cache.notifyRvvSnapshotTestHook()


    var cnt3 = 0;
    s.execute(s"select * from $tableName -- GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs3 = s.getResultSet
    while (rs3.next) {
      cnt3 = cnt3 + 1
    }
    // scalastyle:off
    println("Row count in row buffer after destroy all entries from row buffer but no commit  : " + cnt3)
    // scalastyle:on
    assert(cnt3 == 10, s"Expected row count is 10 while actual row count is $cnt3")


    var cnt4 = 0;
    s.execute(s"select * from SNAPPYSYS_INTERNAL.APP__TESTTABLE_COLUMN_STORE_ -- " +
        s"GEMFIREXD-PROPERTIES executionEngine=Store\n")
    val rs4 = s.getResultSet
    while (rs4.next) {
      cnt4 = cnt4 + 1
    }
    // scalastyle:off
    println("Row count in column store after destroy all entries from row buffer " +
        "and reinitialize snapshot   : " + cnt4)
    // scalastyle:on
    // The number of entries in column store is 4 as after
    // columnwise storage 3 rows will be created one for each
    // column and 4th row is for stats
    assert(cnt4 == 0, s"Expected row count is 0 while actual row count is $cnt4")


  }

  def clearTestHook(netPort: Int): Unit = {

    val cache = GemFireCacheImpl.getInstance()
    if (null != cache) {
      cache.setRvvSnapshotTestHook(null)
      cache.getCacheTransactionManager.testRollBack = false;
    }
  }

}