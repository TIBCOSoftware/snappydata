package io.snappydata.externalstore

import java.io.{File, IOException}
import java.util.Properties

import scala.util.Random

import com.pivotal.gemfirexd.FabricService
import io.snappydata.ServiceManager
import io.snappydata.cluster.{ClusterManagerTestBase, ExecutorInitiator}
import io.snappydata.test.dunit.{SerializableRunnable, DistributedTestBase}
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}

class ExecutorMessageDUnitTest(s: String) extends ClusterManagerTestBase(s) with Logging {

  val tableName = "ExecutorMessageDUnitTest_table"

  def testStoreBlockMapUpdatesWithExecutorDown(): Unit = {
    import ExecutorMessageDUnitTest._
    val snc = SnappyContext(sc)
    executeSomething(snc)
    verifyMap(snc, "stopExecutor")
    restartExecutors(locatorPort, bootProps)
    getLogWriter.info("testStoreBlockMapUpdatesWithExecutorDown() Successful")
  }

  // TODO The next test that runs after this one hangs/fails because we stop the
  // server process in this test. Enable it after resolving the issue (SNAP-907).
  // When run individually, this test passes.
  def _testStoreBlockMapUpdatesWithNodeDown(): Unit = {
    val snc = SnappyContext(sc)
    var props = Map.empty[String, String]
    props += ("REDUNDANCY" -> "2")
    executeSomething(snc, props)
    verifyMap(snc, "stopProcess")
    // stopRest()
    getLogWriter.info("testStoreBlockMapUpdatesWithNodeDown() Successful")
  }

  def executeSomething(snc: SnappyContext,
      props: Map[String, String] = Map.empty[String, String]): Unit = {
    createAndPopulateTable(snc, props)

    val wc: WaitCriterion = new WaitCriterion {
      override def done(): Boolean = {
        SnappyContext.storeToBlockMap.size == 4 // 3 servers + 1 lead/driver
      }
      override def description(): String = {
        s"Received executor message only from" +
            s" ${SnappyContext.storeToBlockMap.size} members, expected from three."
      }
    }
    DistributedTestBase.waitForCriterion(wc, 10000, 500, true)
    assert(SnappyContext.storeToBlockMap.size == 4) // 3 servers + 1 lead/driver
    for ((dm, blockId) <- SnappyContext.storeToBlockMap) {
      assert(blockId != null)
    }
  }

  def createAndPopulateTable(snc: SnappyContext, props: Map[String, String]): Unit = {
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)
  }

  def verifyMap(snc: SnappyContext, m: String): Unit = {
    vm0.invoke(getClass, m)
    assert(SnappyContext.storeToBlockMap.size == 3)
    for ((dm, blockId) <- SnappyContext.storeToBlockMap) {
      assert(blockId != null)
    }
    verifyTable(snc)

    vm1.invoke(getClass, m)
    assert(SnappyContext.storeToBlockMap.size == 2)
    for ((dm, blockId) <- SnappyContext.storeToBlockMap) {
      assert(blockId != null)
    }
    verifyTable(snc)

    vm2.invoke(getClass, m)
    assert(SnappyContext.storeToBlockMap.size == 1)
  }

  def verifyTable (snc: SnappyContext): Unit = {
    assert(snc.sql("SELECT * FROM " + tableName).collect().length == 1005)
  }

  def stopRest(): Unit = {
    import ExecutorMessageDUnitTest._
    // vm3.invoke(getClass, "")
    stopLocatorLead()
  }
}

object ExecutorMessageDUnitTest {

  def stopExecutor(): Unit = {
    ExecutorInitiator.stop()
    Thread.sleep(1000)
  }

  def restartExecutors(locatorPort: Int, props: Properties): Unit = {
    SnappyContext.globalSparkContext.stop()
    ClusterManagerTestBase.startSnappyLead(locatorPort, props)
  }

  def stopProcess(): Unit = {
    // ClusterManagerTestBase.stopAny()
    ClusterManagerTestBase.cleanupTestData(null, null)
    // ClusterManagerTestBase.stopNetworkServers()
    ServiceManager.getServerInstance.stop(null)
    // Thread.sleep(2000)
  }

  def stopLocatorLead(): Unit = {
    // Stop lead
    SnappyContext.globalSparkContext.stop()
    val lead = ServiceManager.getServerInstance
    if (lead.status() == FabricService.State.RUNNING) {
      lead.stop(null)
    }
    // Stop locator
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        val loc = ServiceManager.getLocatorInstance
        if (loc.status() == FabricService.State.RUNNING) {
          loc.stop(null)
        }
      }
    })
    // Delete all oplog files
    deleteOplogFiles(true)
  }

  @throws(classOf[IOException])
  def deleteOplogFiles(includeDataDictionary: Boolean): Unit = {
    try {
      val currDir: File = new File(".")
      val files: Array[File] = currDir.listFiles
      println("current dir is: " + currDir.getCanonicalPath)
      for (f <- files) {
        if (f.getAbsolutePath.contains("BACKUPGFXD-DEFAULT-DISKSTORE")) {
          println("deleting file: " + f + " from dir: " + currDir)
          f.delete
        }
        if (f.isDirectory) {
          val newDir: File = new File(f.getCanonicalPath)
          val newFiles: Array[File] = newDir.listFiles
          for (nf <- newFiles) {
            if (nf.getAbsolutePath.contains("BACKUPGFXD-DEFAULT-DISKSTORE")) {
              println("deleting file: " + nf + " from dir: " + newDir)
              nf.delete
            }
          }
        }
      }
      if (!includeDataDictionary) {
        return
      }
      for (f <- files) {
        if (f.getAbsolutePath.contains("GFXD-DD-DISKSTORE")) {
          println("deleting file: " + f + " from dir: " + currDir)
          f.delete
        }
        if (f.isDirectory) {
          val newDir: File = new File(f.getCanonicalPath)
          val newFiles: Array[File] = newDir.listFiles
          for (nf <- newFiles) {
            if (nf.getAbsolutePath.contains("GFXD-DD-DISKSTORE")) {
              println("deleting file: " + nf + " from dir: " + newDir)
              nf.delete
            }
          }
        }
      }
    }
    catch {
      case e: IOException => {
      }
    }
  }
}
