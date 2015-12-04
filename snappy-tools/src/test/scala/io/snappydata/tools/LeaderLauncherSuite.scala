package io.snappydata.tools

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.util.{Failure, Success, Try}

import com.gemstone.gemfire.internal.AvailablePort
import com.gemstone.gemfire.internal.cache.CacheServerLauncher
import com.pivotal.gemfirexd.{TestUtil, FabricService, Attribute}
import com.pivotal.gemfirexd.tools.GfxdDistributionLocator
import io.snappydata._
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by soubhikc on 6/10/15.
 */
class LeaderLauncherSuite extends SnappyToolFunSuite with BeforeAndAfterAll {

  private val availablePort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)

  override def beforeAll(): Unit = {
    val f = new java.io.File("tests-snappy-loc-dir")
    f.mkdir()
    dirList += f.getAbsolutePath

    CacheServerLauncher.DONT_EXIT_AFTER_LAUNCH = true
    GfxdDistributionLocator.main(Array(
      "start",
      "-dir=" + f.getAbsolutePath,
      s"-peer-discovery-address=localhost",
      s"-peer-discovery-port=${availablePort}"
    ))
  }

  override def afterAll(): Unit = {
    GfxdDistributionLocator.main(Array(
      "stop",
      "-dir=tests-snappy-loc-dir"
    ))
    CacheServerLauncher.DONT_EXIT_AFTER_LAUNCH = false
    dirCleanup()
  }

  test("leader api") {
    val dirname = createDir("tests-snappy-leader-api")
    val fs: Lead = ServiceManager.getLeadInstance

    val props = TestUtil.doCommonSetup(null)

    props.setProperty(Property.locators, s"localhost[${availablePort}]")
    props.setProperty(Attribute.SYS_PERSISTENT_DIR, dirname)
    fs.start(props)

    assert(ServiceManager.getLeadInstance.status == FabricService.State.RUNNING)

    fs.stop(null)
  }

  test("simple leader launch") {
    val dirname = createDir("tests-snappy-leader")
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

  test("leader standby") {

    def verifyStatus(workingDir: String, expectedOutput: String): Try[Unit] = {
      val stream = new ByteArrayOutputStream()
      Try {
        System.setOut(new PrintStream(stream))
        LeaderLauncher.main(Array(
          "status",
          "-dir=" + workingDir))
      } map { _ =>
        val outputLines = stream.toString
        assert(outputLines.replaceAll("\n", "").matches(expectedOutput),
          workingDir + " returned with: \n" + outputLines)
      }
    }

    val leader1 = createDir("tests-snappy-leader-1")
    val leader2 = createDir("tests-snappy-leader-2")
    val currentOut = System.out

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader1,
        s"-locators=localhost[${availablePort}]"
      ))
    } transform(_ => Try {

      verifyStatus(leader1, "SnappyData Leader pid: [0-9]+ status: running.*").get

      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader2,
        s"-locators=localhost[${availablePort}]"
      ))
    }, {
      throw _
    })

    var isLeader1NotStopped = true
    try {
      val checkStandby = start transform(_ => {
        verifyStatus(leader2, "SnappyData Leader pid: [0-9]+ status: standby.*")
      }, throw _)


      val leader2TakeOver = checkStandby match {
        case Success(v) =>
          Try {
            LeaderLauncher.main(Array(
              "stop",
              "-dir=" + leader1))
            isLeader1NotStopped = false
          } transform(_ => {
            verifyStatus(leader2, "SnappyData Leader pid: [0-9]+ status: running.*")
          }, throw _)

        case Failure(t) => throw t
      }

      leader2TakeOver match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      System.setOut(currentOut)
      if (isLeader1NotStopped) {
        LeaderLauncher.main(Array(
          "stop",
          "-dir=" + leader1
        ))
      }

      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + leader2
      ))
    }
  }

  test("leader startup using SparkContext") {
    val dirname = createDir("tests-snappy-leader-by-conf")

    val conf = new SparkConf()
        .setAppName(testName)
        .setMaster(Constant.JDBC_URL_PREFIX + s"localhost[${availablePort}]")
        // .set(Prop.Store.locators, s"localhost[${availablePort}]")
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.SYS_PERSISTENT_DIR, dirname)

    val sc = new SparkContext(conf)

    sc.stop()
  }

  test("simple leader spark properties") {

    val dirname = createDir("tests-snappy-leader-spark-prop")

    try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + dirname,
        s"-locators=localhost[${availablePort}]",
        s"-spark.ui.port=3344",
        s"-jobserver.enabled=true",
        s"-embedded=true"
      ))
    } finally {
      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + dirname
      ))
    }

  }

}
