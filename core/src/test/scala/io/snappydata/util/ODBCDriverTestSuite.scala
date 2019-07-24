
package io.snappydata
import java.io._

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._


class ODBCDriverTestSuite extends SnappyTestRunner {
  val snappyProductDir = System.getenv("SNAPPY_HOME")
  test("ODBC_FailOverTest_NEWSERVER"){
    try {
      var scriptPath = snappyProductDir +
          "/../../../store/native/tests/unitTest.sh "
      var scriptDirPath = snappyProductDir +
          "/../../../store/native/tests/"
      var consoleOutput = (scriptPath
          + snappyProductDir + " " + scriptDirPath).!!
      assert(consoleOutput.contains("after"),
        "Failed")
    } finally {

    }
  }
  test("ODBC_FailOverTest_NONE"){
    try {
      var scriptPath = snappyProductDir +
          "/../../../store/native/tests/unitTest_2.sh "
      var scriptDirPath = snappyProductDir +
          "/../../../store/native/tests/"
      var consoleOutput = (scriptPath
          + snappyProductDir + " " + scriptDirPath).!!
      assert(consoleOutput.contains("Exception"),
        "Failed to connect to same server")
    } finally {

    }
  }
}
