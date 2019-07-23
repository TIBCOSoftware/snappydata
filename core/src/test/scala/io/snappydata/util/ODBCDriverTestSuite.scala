
package io.snappydata
import java.io._

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._


class ODBCDriverTestSuite extends SnappyTestRunner {
  val snappyProductDir = System.getenv("SNAPPY_HOME")
  test("ODBC_FailOverTest"){
    try {
      // cluster has been started already
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir not specified: $consoleOutput")
      var scriptPath = snappyProductDir +
          "/../../../store/native/tests/unitTest.sh "
      var scriptDirPath = snappyProductDir +
          "/../../../store/native/tests/"
//      var scriptPath = "./unitTest.sh "
      consoleOutput = (scriptPath
          + snappyProductDir + " " + scriptDirPath).!!
      assert(consoleOutput.contains("after"),
        "Failed")
    } finally {
      (snappyProductDir +
          "/sbin/snappy-stop-all.sh").!!
      var scriptDirPath = snappyProductDir +
          "/../../../store/native/tests/"
    }
  }
}
