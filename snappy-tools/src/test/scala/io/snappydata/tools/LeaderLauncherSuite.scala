package io.snappydata.tools

import scala.util.Try

import com.gemstone.gemfire.internal.{DistributionLocator, AvailablePort}
import com.pivotal.gemfirexd.tools.GfxdDistributionLocator
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Created by soubhikc on 6/10/15.
 */
class LeaderLauncherSuite extends SnappyFunSuite with BeforeAndAfterAll {

  private val availablePort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)

  override def beforeAll(): Unit = {
    new java.io.File("snappy-loc-dir").mkdir()
    System.setProperty("gemfire.CacheServerLauncher.dontExitAfterLaunch", "true")

    GfxdDistributionLocator.main(Array(
      "start",
      "-dir=snappy-loc-dir",
      s"-peer-discovery-port=${availablePort}"
    ))
  }

  override def afterAll(): Unit = {
    GfxdDistributionLocator.main(Array(
      "stop",
      "-dir=snappy-loc-dir"
    ))
    new java.io.File("snappy-loc-dir").delete()
    System.setProperty("gemfire.CacheServerLauncher.dontExitAfterLaunch", "false")
  }

  test("simple leader launch") {
    new java.io.File("snappy-leader").mkdir()

    Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=snappy-leader",
        s"-locators=localhost[${availablePort}]"
      ))
    } transform ({ case _ =>
      Try {
        LeaderLauncher.main(Array(
          "stop",
          "-dir=snappy-leader"
        ))
      }
    }, {throw _})

  }
}
