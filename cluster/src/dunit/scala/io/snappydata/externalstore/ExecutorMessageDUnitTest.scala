package io.snappydata.externalstore

import java.io.{File, IOException}
import java.util.Properties

import com.pivotal.gemfirexd.FabricService
import io.snappydata.{Locator, ServiceManager}
import io.snappydata.cluster.{ClusterManagerTestBase, ExecutorInitiator}
import io.snappydata.test.dunit.{SerializableRunnable, DistributedTestBase}
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext

class ExecutorMessageDUnitTest(s: String) extends ClusterManagerTestBase(s) with Logging {

  def testStore01BlockMapUpdatesWithExecutorDown(): Unit = {
    import ExecutorMessageDUnitTest._
    executeSomething()
    verifyMap("stopExecutor")
    restartExecutors(locatorPort, bootProps)
    getLogWriter.info("testStore01BlockMapUpdatesWithExecutorDown() Successful")
  }

  // TODO The next test that runs after this one hangs/fails because we stop the
  // server process in this test. Enable it after resolving the issue.
  // When run individually, this test passes.
  def _testStore02BlockMapUpdatesWithNodeDown(): Unit = {
    executeSomething()
    verifyMap("stopProcess")
    // stopRest()
    getLogWriter.info("testStore02BlockMapUpdatesWithNodeDown() Successful")
  }

  def executeSomething(): Unit = {
    val snc = SnappyContext(sc)

    val wc: WaitCriterion = new WaitCriterion {
      override def done(): Boolean = {
        SnappyContext.storeToBlockMap.size == 3
      }
      override def description(): String = {
        s"Received executor message only from" +
            s" ${SnappyContext.storeToBlockMap.size} members, expected from three."
      }
    }
    DistributedTestBase.waitForCriterion(wc, 10000, 500, true)
    assert(SnappyContext.storeToBlockMap.size == 3)
    for ((dm, blockId) <- SnappyContext.storeToBlockMap) {
      assert(blockId != null)
    }
  }

  def verifyMap(m: String): Unit = {
    vm0.invoke(getClass, m)
    assert(SnappyContext.storeToBlockMap.size == 2)
    for ((dm, blockId) <- SnappyContext.storeToBlockMap) {
      assert(blockId != null)
    }

    vm1.invoke(getClass, m)
    assert(SnappyContext.storeToBlockMap.size == 1)
    for ((dm, blockId) <- SnappyContext.storeToBlockMap) {
      assert(blockId != null)
    }

    vm2.invoke(getClass, m)
    assert(SnappyContext.storeToBlockMap.size == 0)
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
    // Thread.sleep(2000)
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
