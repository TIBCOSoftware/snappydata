
package io.snappydata
import java.io._

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._


class ODBCDriverTestSuite extends SnappyTestRunner {
  val snappyProductDir = System.getenv("SNAPPY_HOME")
  val snappyNativeTestDir = s"$snappyProductDir/../../../store/native/tests"

  test("ODBC_FailOverTest_NEWSERVER"){
    try {
      var scriptPath = s"$snappyNativeTestDir/failoverTest_NewServer.sh"
      var consoleOutput = s"$scriptPath $snappyProductDir $snappyNativeTestDir".!!
      assert(consoleOutput.contains("Test executed successfully"),
        s"FailOver failed $consoleOutput")
    } finally {

    }
  }

  test("ODBC_FailOverTest_NONE"){
    try {
      var scriptPath = s"$snappyNativeTestDir/failoverTest_None.sh"
      var consoleOutput = s"$scriptPath $snappyProductDir $snappyNativeTestDir".!!
      assert(consoleOutput.contains("Test executed successfully, no failover tried"),
        s"There failover tried but failed $consoleOutput")
    } finally {

    }
  }
}
