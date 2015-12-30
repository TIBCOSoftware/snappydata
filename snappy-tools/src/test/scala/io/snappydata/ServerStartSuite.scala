package io.snappydata

import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricService, TestUtil}
import org.apache.spark.sql.SnappyContext
import org.scalatest.BeforeAndAfterAll

/**
  * Created by hemantb.
  */
class ServerStartSuite extends SnappyFunSuite with BeforeAndAfterAll {
  var props: Properties = null

  override def beforeAll(): Unit = {
    SnappyContext.stop()

    Class.forName("org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager$") //scalastyle:ignore
    props = TestUtil.doCommonSetup(null)
    GemFireXDUtils.IS_TEST_MODE = true
  }

  override def afterAll(): Unit = {
    GemFireXDUtils.IS_TEST_MODE = false
    dirCleanup()
  }

  test("Snappy Server start") {
    val fs: Server = ServiceManager.getServerInstance

    fs.start(props)

    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)

    fs.stop(null)
  }

  ignore("Snappy Lead start") {
    val fs: Lead = ServiceManager.getLeadInstance

    // right version of the test is now in LeaderLauncherSuite.
    try {
      fs.start(props)
      fail("must fail as locator info not present")
    } catch {
      case e: Exception =>
        var ex: Throwable = e
        var found = false
        while (ex != null) {
          if (ex.getMessage.contains("locator info not provided in the snappy embedded url")) {
            found = true
          }
          ex = ex.getCause
        }
        if (!found) {
          throw e
        }
      case other: Throwable => throw other
    }

  }

  test("Snappy Locator start") {
    val fs: Locator = ServiceManager.getLocatorInstance

    fs.start(FabricLocator.LOCATOR_DEFAULT_BIND_ADDRESS, FabricLocator.LOCATOR_DEFAULT_PORT, props)

    assert(ServiceManager.getLocatorInstance.status == FabricService.State.RUNNING)

    fs.stop(null)
  }

}
