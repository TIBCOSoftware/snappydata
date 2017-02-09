package io.snappydata.cluster

import java.io.File
import java.sql.{DriverManager, Connection, DatabaseMetaData, SQLException, Statement}
import java.util
import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl.RvvSnapshotTestHook
import com.gemstone.gemfire.internal.cache.{BucketRegion, PartitionedRegion, GemFireCacheImpl, TXState}
import com.gemstone.gemfire.internal.cache.versions.{VersionSource, RegionVersionHolder}
import com.pivotal.gemfirexd.{TestUtil, FabricService}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.Constant._
import io.snappydata.{ServiceManager, Locator}
import io.snappydata.test.dunit.{DistributedTestBase, VM, AvailablePortHelper, SerializableRunnable}
import junit.framework.TestCase
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.slf4j.LoggerFactory

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{SaveMode, SnappyContext}

class ValidateMVCCDUnitTest(val s: String) extends ClusterManagerTestBase(s) with Logging {

  // set default batch size for this test
  bootProps.setProperty(io.snappydata.Property.CachedBatchSize.name, "4")

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
    //vm1.invoke(startNode)
    //vm2.invoke(startNode)

    // start lead node in this VM
    val sc = SnappyContext.globalSparkContext
    if (sc == null || sc.isStopped) {
      ClusterManagerTestBase.startSnappyLead(locPort, bootProps)
    }
    assert(ServiceManager.currentFabricServiceInstance.status ==
        FabricService.State.RUNNING)
  }




  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def testMVCCForColumnTable(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)



    val snc = SnappyContext(sc)
    val tableName: String = "TESTTABLE"
    snc.sql("set spark.sql.inMemoryColumnarStorage.batchSize = 2")


    snc.sql(s"create table $tableName(col1 integer, col2 String, col3 integer) using column " +
        s"OPTIONS (PARTITION_BY 'col1', buckets '1',MAXPARTSIZE '200')")

    //Invoking validate result in each VM as a separate thread inorder to resume the code for
    // insertion of records
    invokeMethodInVm(vm0, classOf[ClusterManagerTestBase], "validateResults", netPort1)


    val rdd1 = sc.parallelize(
      (1 to 5).map(i => Data(i, i.toString, Decimal(i.toString + '.' + i))))

    val dataDF1 = snc.createDataFrame(rdd1)
    //Write 5 records as batch size is set to 2 it will trigger the cachebatch creation
    dataDF1.write.insertInto(tableName)

    val cnt = snc.sql(s"select * from $tableName").count()

    assert(cnt == 5)
    println("Successful")

  }

  def invokeMethodInVm(vM: VM, classType: Class[ClusterManagerTestBase], methodName: String, netPort1: Int): Unit = {

    new Thread {
      override def run: Unit = {
        vM.invoke(classType, methodName, netPort1)
      }
    }.start()

  }
}
