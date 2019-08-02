/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata

import java.io._

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._

class CommandLineToolsSuite extends SnappyTestRunner {

  override def servers: String = s"$localHostName\n"

  override def clusterSuccessString: String = "Distributed system now has 3 members"

  private val snappyProductDir = System.getenv("SNAPPY_HOME")
  private val snappyNativeTestDir = s"$snappyProductDir/../../../store/native/tests"

  // scalastyle:off println
  test("backup restore") {
    val debugWriter = new PrintWriter(s"$snappyHome/CommandLineToolsSuite.debug")
    val backupDir = new File(s"/tmp/backup_dir.${System.currentTimeMillis()}")
    try {
      SnappyShell("quickStartScripts", Seq("connect client 'localhost:1527';",
        s"create table test_app (col1 int not null, col2 int not null) using column options();;",
        s"insert into test_app values (1, 1), (2, 2);",
        s"insert into test_app values (5, 3), (6, 4);",
        s"create table testDD (col1 int not null, col2 int not null)" +
            s" using row options(DISKSTORE 'GFXD-DD-DISKSTORE');",
        s"insert into testDD values (1, 1), (2, 2);",
        s"insert into testDD values (5, 3), (6, 4);",
        "exit;"))

      if (backupDir.exists) {
        assert(backupDir.delete(), s"could not delete $backupDir")
      }
      assert(backupDir.mkdir(), s"could not create backup dir in $snappyHome")

      // online backup command
      val backupcommand = s"$snappyHome/bin/snappy backup $backupDir -locators=localhost:10334"
      val (out, _) = executeCommand(backupcommand)

      if (!out.contains("successful")) {
        throw new Exception(s"Could not take successful backup")
      }
      stopCluster()

      val (_, err1) = executeCommand(s"rm -rf $snappyHome/work")

      if (err1 != null && err1.length > 0) {
        throw new Exception(s"Failed to remove work dir")
      }
      // Find all the restore scripts
      val (out3, _) = executeCommand(s"find $backupDir -name restore.sh")

      val restoreCmnds = out3.split("\n")
      assert(restoreCmnds.length == 2, "expected 2 restore commands")
      assert(restoreCmnds(0).contains("restore.sh") && restoreCmnds(1).contains("restore.sh"))

      executeCommand(restoreCmnds(0))
      executeCommand(restoreCmnds(1))

      startupCluster()

      val conn = getJdbcConnection(1527)
      val stmnt = conn.createStatement()
      assert(stmnt.execute("select * from test_app"))
      val rs1 = stmnt.getResultSet
      var cnt = 0
      while (rs1.next()) {
        cnt = cnt + 1
        val v1 = rs1.getInt(1)
        rs1.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6)
      }
      assert(cnt === 4)

      assert(stmnt.execute("select * from testDD"))
      val rs2 = stmnt.getResultSet
      cnt = 0
      while (rs2.next()) {
        cnt = cnt + 1
        val v1 = rs2.getInt(1)
        rs2.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6)
      }
      assert(cnt === 4)

      // add more data in both the table
      stmnt.execute("insert into test_app values (100, 100), (200, 200)")
      stmnt.execute("insert into testDD values (100, 100), (200, 200)")

      // online incremental backup command
      val incre_backupcommand = s"$snappyHome/bin/snappy" +
          s" backup -baseline=$backupDir $backupDir -locators=localhost:10334"
      val (out4, _) = executeCommand(incre_backupcommand)

      if (!out4.contains("successful")) {
        throw new Exception(s"Could not take successful backup")
      }
      stopCluster()
      val (_, err5) = executeCommand(s"rm -rf $snappyHome/work")
      if (err5 != null && err5.length > 0) {
        throw new Exception(s"Failed to remove work dir")
      }

      debugWriter.println(s"backup dir  = $backupDir")
      // Find the latest two restore scripts
      // val backupDirFile = new File(backupDir)
      val backupDirs = backupDir.listFiles()
      assert(backupDirs.length == 2)

      val dir1 = new File(backupDir.getAbsolutePath, backupDirs(0).getName)
      val dir2 = new File(backupDir.getAbsolutePath, backupDirs(1).getName)

      var lastbackDir: File = null
      if (dir2.lastModified() > dir1.lastModified()) {
        lastbackDir = dir2
      }
      else {
        lastbackDir = dir1
      }

      debugWriter.println(s"lastBackDir abs path = ${lastbackDir.getAbsolutePath}")

      val (out6, _) = executeCommand(s"find ${lastbackDir.getAbsolutePath} -iname restore.sh")

      val restoreCmnds2 = out6.split("\n")
      assert(restoreCmnds2.length == 2, "expected 2 restore commands")

      debugWriter.println(s"after incre restore1  = ${restoreCmnds2(0)}")
      debugWriter.println(s"after incre restore2  = ${restoreCmnds2(1)}")

      assert(restoreCmnds2(0).contains("restore.sh") && restoreCmnds(1).contains("restore.sh"))

      executeCommand(restoreCmnds2(0))
      executeCommand(restoreCmnds2(1))

      startupCluster()

      val conn2 = getJdbcConnection(1527)
      val stmnt2 = conn2.createStatement()
      assert(stmnt2.execute("select * from test_app"))
      val rs11 = stmnt2.getResultSet
      cnt = 0
      while (rs11.next()) {
        cnt = cnt + 1
        val v1 = rs11.getInt(1)
        rs11.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6 || v1 === 100 || v1 === 200)
      }
      assert(cnt === 6)

      assert(stmnt2.execute("select * from testDD"))
      val rs22 = stmnt2.getResultSet
      cnt = 0
      while (rs22.next()) {
        cnt = cnt + 1
        val v1 = rs22.getInt(1)
        rs22.getInt(2)
        assert(v1 === 1 || v1 === 2 || v1 === 5 || v1 === 6 || v1 === 100 || v1 === 200)
      }
      assert(cnt === 6)
    } finally {
      debugWriter.close()
      executeCommand(s"rm -rf $snappyHome/backup*")
    }
  }

  test("-dir option with old locator launch script") {
    try {
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir not specified: $consoleOutput")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443 -client-port=2000 -dir=").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified with a value")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443 -client-port=2000 -dir= " +
          "/does/not/exist").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir does not exist $consoleOutput")

      "mkdir ./SNAP-2631-work-locator".!!
      consoleOutput = (snappyProductDir +
          "/sbin/snappy-locator.sh start -peer-discovery-port=10443 -client-port=2000 " +
          "-dir=./SNAP-2631-work-locator").!!
      assert(consoleOutput.contains("running"), s"Locator launch failed: $consoleOutput")

    } finally {
      (snappyProductDir +
          "/sbin/snappy-locator.sh stop -dir=./SNAP-2631-work-locator").!!
      "rm -r ./SNAP-2631-work-locator".!!
    }
  }

  test("-dir option with old server launch script") {
    try {
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir not specified: $consoleOutput")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001 -dir=").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified with a value")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001 -dir= " +
          "/does/not/exist").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir does not exist $consoleOutput")

      "mkdir ./SNAP-2631-work-server".!!
      consoleOutput = (snappyProductDir +
          "/sbin/snappy-server.sh start -locators=localhost:10334 -client-port=2001  " +
          "-dir=./SNAP-2631-work-server").!!
      assert(consoleOutput.contains("running"), s"Server launch failed: $consoleOutput")

    } finally {
      (snappyProductDir +
          "/sbin/snappy-server.sh stop -dir=./SNAP-2631-work-server").!!
      "rm -r ./SNAP-2631-work-server".!!
    }
  }

  test("-dir option with old lead launch script") {
    try {
      var consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002" +
          " -dir=").!!
      assert(consoleOutput.contains("ERROR"),
        "Option -dir not specified with a value")

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002 -dir= " +
          "/does/not/exist").!!
      assert(consoleOutput.contains("ERROR"),
        s"Option -dir does not exist $consoleOutput")

      "mkdir ./SNAP-2631-work-lead".!!

      consoleOutput = (snappyProductDir +
          "/sbin/snappy-lead.sh start -locators=localhost:10334 -client-port=2002 " +
          "-dir=./SNAP-2631-work-lead").!!

      assert(consoleOutput.contains("standby"),
        s"lead launch failed: $consoleOutput")

    } finally {
      (snappyProductDir +
          "/sbin/snappy-lead.sh stop -dir=./SNAP-2631-work-lead").!!
      "rm -r ./SNAP-2631-work-lead".!!
    }
  }

  test("test_run_command") {
    val out = new StringBuilder
    val code = (snappyProductDir +
        "/bin/snappy run file='somefile.sql'").!(ProcessLogger(s => out.append(s)))
    assert(code != 0)
    assert(!out.toString().contains("[-locators=<addresses>]"),
      s"-locators option still displayed in run command's usage text!")
  }

  ignore("ODBC_FailOverTest_NEWSERVER"){
    try {
      var scriptPath = s"$snappyNativeTestDir/failoverTest_NewServer.sh"
      var consoleOutput = s"$scriptPath $snappyProductDir $snappyNativeTestDir".!!
      assert(consoleOutput.contains("Test executed successfully"),
        s"FailOver failed $consoleOutput")
    } finally {

    }
  }

  ignore("ODBC_FailOverTest_NONE"){
    try {
      var scriptPath = s"$snappyNativeTestDir/failoverTest_None.sh"
      var consoleOutput = s"$scriptPath $snappyProductDir $snappyNativeTestDir".!!
      assert(consoleOutput.contains("Test executed successfully, no failover tried"),
        s"There failover tried but failed $consoleOutput")
    } finally {

    }
  }
}
