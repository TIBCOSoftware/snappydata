package io.snappydata.tools

import java.io.{PrintStream, ByteArrayOutputStream}

import scala.util.{Failure, Try}

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
    val f = new java.io.File("snappy-loc-dir");
    f.mkdir()
    System.setProperty("gemfire.CacheServerLauncher.dontExitAfterLaunch", "true")

    GfxdDistributionLocator.main(Array(
      "start",
      "-dir=" + f.getAbsolutePath,
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
    val dirname = "snappy-leader"
    new java.io.File(dirname).mkdir()
    val stream = new ByteArrayOutputStream()

    val currentOut = System.out

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + dirname,
        s"-locators=localhost[${availablePort}]"
      ))
    }

    try {
      start transform( { _ =>
        Try {
          System.setOut(new PrintStream(stream))
          LeaderLauncher.main(Array(
            "status",
            "-dir=" + dirname))
        } map { _ =>
          val outputLines = stream.toString
          assert(outputLines.replaceAll("\n", "").matches(
            "SnappyData Leader pid: [0-9]+ status: running" +
                "  Distributed system now has [0-9]+ members." +
                "  Other members: .*([0-9]+:.*)<.*>:[0-9]+".r), outputLines)

        }
      }, {
        throw _
      }) match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      System.setOut(currentOut)
      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + dirname
      ))
    }

  }

  ignore("leader standby") {

    class conf(val dirname: String) {
      def createDir(): conf = {
        new java.io.File(dirname).mkdir()
        this
      }
    }

    val leader1 = new conf("snappy-leader-1").createDir()
    val leader2 = new conf("snappy-leader-2").createDir()

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader1.dirname,
        s"-locators=localhost[${availablePort}]"
      ))
    } transform(_ => Try {
      LeaderLauncher.main(Array(
        "server",
        "-log-file=" + leader2.dirname + "leader2.log",
        s"-locators=localhost[${availablePort}]"
      ))
    }, throw _) orElse
        // stop the first leader when second leader failed to start
        Try {
          LeaderLauncher.main(Array(
            "stop",
            "-dir=" + leader1.dirname))
        }

    val stream = new ByteArrayOutputStream()
    val currentOut = System.out

    try {
      start transform(_ =>
        Try {
          System.setOut(new PrintStream(stream))
          LeaderLauncher.main(Array(
            "status",
            "-dir=" + leader2.dirname))
        } map { _ =>
          val outputLines = stream.toString
          assert(outputLines.replaceAll("\n", "").matches(
            "SnappyData Leader pid: [0-9]+ status: standby" + "".r), outputLines)

        }
          , throw _) match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      System.setOut(currentOut)
      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + leader1.dirname
      ))

      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + leader2.dirname
      ))

    }


  }
}
