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

import java.io.{BufferedOutputStream, ByteArrayOutputStream, File, PrintStream, PrintWriter}
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import scala.sys.process.{Process, ProcessLogger, stderr, stdout}
import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import org.apache.spark.sql.collection.Utils
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.Logging
import org.apache.spark.sql.udf.UserDefinedFunctionsDUnitTest.{createJarFile, createUDFClass}

class RecoveryTestSuite extends SnappyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  val adminUser1 = "gemfire10"
  private val commandOutput = "command-output.txt"
  // todo: is it ok to declare ports here?? ports are common to all tests?
  private val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
  private var confDirPath = ""
  private var workDirPath = ""
  private var recovery_mode_dir = ""
  private var test_status: Boolean = false
  private[this] var ldapProperties: Properties = new Properties()

  def getLdapConf: String = {
    var conf = ""
    for (k <- List(Attribute.AUTH_PROVIDER, Attribute.USERNAME_ATTR, Attribute.PASSWORD_ATTR)) {
      conf += s"-$k=${ldapProperties.getProperty(k)} "
    }
    for (k <- List(AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      conf += s"-J-D$k=${ldapProperties.getProperty(k)} "
    }
    conf // + "-J-DDistributionManager.VERBOSE=true "
  }

  def clearDirectory(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(clearDirectory(_))
    }
    if (dir.exists() && !dir.delete()) {
      throw new Exception("Error clearing Directory/File" + dir.getAbsolutePath)
    }
  }

  /**
   * start LDAP server in beforeAll
   */
  override def beforeAll(): Unit = {

    recovery_mode_dir = System.getProperty("RECOVERY_TEST_DIR")
    logInfo("recovery_mode_dir: " + recovery_mode_dir)
    RecoveryTestSuite.snappyHome = System.getenv("SNAPPY_HOME")
    if (RecoveryTestSuite.snappyHome == null) {
      throw new Exception("SNAPPY_HOME should be set as an environment variable")
    }

    // start LDAP server
    logInfo("Starting LDAP server")

    // starts LDAP server and sets LDAP properties to be passed to conf files
    setSecurityProps()

  }

  def setSecurityProps(): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0,
      adminUser1, getClass.getResource("/auth.ldif").getPath)
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
  }

  override def afterAll(): Unit = {
    // 1. stop  ldap cluster.
    stopLdapTestServer
    // 2. delete all
  }

  def stopLdapTestServer(): Unit = {
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    val confDir = new File(confDirPath)
    val workDir = new File(workDirPath)


    // 1. Stop the cluster - before clearing conf - as it is required by stop script
    stopCluster()

    // 2. check if the test is successful (test_status flag)- then clear the conf dir and work dir
    // 3. if the test is not successful - rename (prefix "FAILED_TEST") the
    // conf dir and work dir so that it doesn't get clear as part of afterAll()
    // cleat confDirPath and workDirPath variables

    // TODO: remove the negation below after testing
    if (test_status) {
      logInfo("Clearing conf and work dir.")
      clearDirectory(confDir)
      clearDirectory(workDir)
    }


    // 4. reset test_status flag to false
    test_status = false
  }

  def startSnappyCluster(): Unit = {
    val (out, _) = RecoveryTestSuite.executeCommand(s"${RecoveryTestSuite.snappyHome}" +
        s"/sbin/snappy-start-all.sh --config $confDirPath")

    // TODO need a better way to ensure the cluster has started
    if (!out.contains("Distributed system now")) {
      throw new Exception(s"Failed to start Snappy cluster.")
    }
  }


  def basicOperationSetSnappyCluster(stmt: Statement, defaultSchema: String = "APP"): Unit = {
    // 1. create new schema
    // 2. create tables in new schema - both row and column
    // 3. create UDF

    stmt.execute(
      s"""
        CREATE TABLE $defaultSchema.testtab1 (
          col1 Int, col2 String, col3 Decimal
        ) USING Row""")
    stmt.execute(s"insert into $defaultSchema.testtab1 values(1,'aaaa',2.2)")
    stmt.execute(s"insert into $defaultSchema.testtab1 values(2,'bbbb',3.3)")
    stmt.execute(s"create view $defaultSchema.vw_testtab1 AS " +
        s"(select * from $defaultSchema.testtab1)")
    stmt.execute("create schema tapp")
    stmt.execute("create table tapp.coltab (col1 int, col2 int, col3 varchar(22))" +
        " using column options()")
    stmt.execute("create table tapp.rowtab (col1 int, col2 int, col3 varchar(22))" +
        " using row options()")

    stmt.execute("create diskstore anotherdiskstore")
    stmt.execute("create table tapp.tcol (col1 int not null," +
        " col2 int not null) using column options(diskstore 'anotherdiskstore')")

    val udfText: String = "public class IntegerUDF implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    val file = createUDFClass("IntegerUDF", udfText)
    val jar = createJarFile(Seq(file))

    stmt.execute(s"CREATE FUNCTION $defaultSchema.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")

    stmt.executeQuery(s"select *,intudf(col2) as newcol from $defaultSchema.testtab1")

    // || Remains || //

    // 4. create diskstore ...
    // 5. create tables/schema/functions/views in new diskstore
    //
  }

  def stopCluster(): Unit = {
    // TODO need a way to ensure the cluster has stopped
    RecoveryTestSuite.executeCommand(s"${RecoveryTestSuite.snappyHome}" +
        s"/sbin/snappy-stop-all.sh --config $confDirPath")
  }

  def startSnappyRecoveryCluster(): Unit = {
    val (out, _) = RecoveryTestSuite.executeCommand(s"${RecoveryTestSuite.snappyHome}" +
        s"/sbin/snappy-start-all.sh --recover --config $confDirPath")

    // TODO need a better way to ensure the cluster has started
    if (!out.contains("Distributed system now")) {
      throw new Exception(s"Failed to start Snappy cluster in recovery mode.")
    }
  }

  def createConfDir(testName: String): String = {
    val confdir = createFileDir(recovery_mode_dir + File.separator + "conf_" + testName)
    confdir.getAbsolutePath
  }

  def createFileDir(absDirPath: String): File = {
    val dir = new File(absDirPath)
    if (!dir.exists()) {
      dir.mkdir()
    }
    if (!dir.exists()) {
      throw new Exception("Error creating custom work directory at " + recovery_mode_dir)
    }
    dir
  }

  def createWorkDir(testName: String, leadsNum: Int, locatorsNum: Int, serversNum: Int): String = {

    // work dir
    val workDir = createFileDir(recovery_mode_dir + File.separator + "work_" + testName)

    // leads dir inside work dir
    for (i: Int <- 1 to leadsNum) {
      createDir(recovery_mode_dir + File.separator +
          "work_" + testName + File.separator + s"lead-$i")
    }

    // locators dir inside work dir
    for (i <- 1 to locatorsNum) {
      createDir(recovery_mode_dir + File.separator +
          "work_" + testName + File.separator + s"locator-$i")
    }

    // servers dir inside work dir
    for (i <- 1 to serversNum) {
      createDir(recovery_mode_dir + File.separator +
          "work_" + testName + File.separator + s"server-$i")
    }
    workDir.getAbsolutePath
  }

  def writeToFile(str: String, fileName: String): Unit = {
    val pw = new PrintWriter(fileName)
    try {
      pw.write(str)
      pw.flush()
    } finally {
      pw.close()
    }
    // wait until file becomes available (e.g. running on NFS)
    var matched = false
    while (!matched) {
      Thread.sleep(100)
      try {
        val source = scala.io.Source.fromFile(fileName)
        val lines = try {
          source.mkString
        } finally {
          source.close()
        }
        matched = lines == str
      } catch {
        case NonFatal(_) =>
      }
    }
  }


  def getConn(port: Int, user: String = "", password: String = ""): Connection = {
    if (user.isEmpty && password.isEmpty) {
      RecoveryTestSuite.getJdbcConnection(port)
    } else {
      val driver = "io.snappydata.jdbc.ClientDriver"
      Utils.classForName(driver).newInstance
      val url: String = "jdbc:snappydata://localhost:" + port + "/"
      DriverManager.getConnection(url, user, password)
    }
  }


  test("test1 - Basic test to list tables names, schemas names and UDFs using LDAP") {

    logInfo("Recovery Test Dir: " + System.getProperty("RECOVERY_TEST_DIR")
        + "\n" + recovery_mode_dir)

    logInfo("PP:RecoveryModeTestSuite: inside test1:\n test status flag: " + test_status)

    // set separate work directory and conf directory

    // 1. make Conf dir inside the recovery_mode

    confDirPath = createConfDir("test1");

    // 2. make workdir inside recovery_mode

    val leadsNum = 1
    val locatorsNum = 1
    val serversNum = 1
    workDirPath = createWorkDir("test1", leadsNum, locatorsNum, serversNum)

    // 3. create conf files with required configuration, as required by the test,
    // inside the tempConf dir - also mention the new work dir as a config

    val waitForInit = "-jobserver.waitForInitialization=true"
    val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort

    val locNetPort = locatorNetPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
    val ldapConf = getLdapConf
    writeToFile(s"localhost  -peer-discovery-port=$locatorPort -dir=$workDirPath/locator-1" +
        s" -client-port=$locNetPort $ldapConf", s"$confDirPath/locators")
    writeToFile(s"localhost  -locators=localhost[$locatorPort]  -dir=$workDirPath/lead-1" +
        s" $waitForInit $ldapConf", s"$confDirPath/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$locatorPort] -dir=$workDirPath/server-1 -client-port=$netPort2 $ldapConf
         |""".stripMargin, s"$confDirPath/servers")


    startSnappyCluster()
    val conn = getConn(locNetPort, "gemfire10", "gemfire10")
    val stmt = conn.createStatement()
    basicOperationSetSnappyCluster(stmt, "gemfire10")

    val rs = stmt.executeQuery("select * from gemfire10.testtab1")

    logInfo("\n")
    logInfo("=== select * from testtab1 === \n")

    while (rs.next()) {
      logInfo(rs.getString("col2"))
    }
    rs.close()

    logInfo("\n")
    logInfo("\n")
    stmt.close()
    conn.close()


    stopCluster()

    startSnappyRecoveryCluster()

    // 1. do a show query and check if catalog is populated properly

    val connRec = getConn(locNetPort, "gemfire10", "gemfire10")
    val stmtRec = connRec.createStatement()

    // enable this once data can be read from file.
    /*
        val rs1 = stmtRec.executeQuery("select * from gemfire10.testtab1")
        logInfo("=== Recovery mode ============\n")
        logInfo("=== select * from testtab1 ===\n")

        while(rs1.next()){
          logInfo(rs1.getString("col2"))
        }
        rs1.close()
    */

    try {
      val rs2 = stmtRec.executeQuery("show tables in gemfire10")
      logInfo("tableNames in gemfire10:\n")
      while (rs2.next()) {
        val c2 = rs2.getString("tableName")
        logInfo(c2)
      }
      rs2.close()

      val rs3 = stmtRec.executeQuery("show tables in tapp")
      logInfo("tableNames in tapp:\n")
      while (rs3.next()) {
        val c2 = rs3.getString("tableName")
        logInfo(c2)
      }
      rs3.close()

      val rs4 = stmtRec.executeQuery("show functions")
      logInfo("Functions :\n")
      while (rs4.next()) {
        logInfo(rs4.getString("function"))
      }
      rs4.close()

    } finally {
      stmtRec.close()
      connRec.close()
    }

    logInfo("\n")
    logInfo("\n")

    // check if all the contents that are expected to be available
    // to user is present for user to choose.

    // After the cluster has come up and ready to be used by user.
    // check if all procedures available to user is working fine

    // at the end ==== set the test_status flag to true
    test_status = true
  }

/*
  test("test2 - Does all basic tests in non-secure mode(without LDAP).") {
    // although ldap server is started before all, if ldap properties are not passed to conf,
    // it should work in non-secure mode.
    // basicOperationSetSnappyCluster can be used


    // check for row and column type
    // check if all the contents that are expected to be available to user is present for user to choose

    // After the cluster has come up and ready to be used by user.
    // check if all procedures available to user is working fine
  }

  test("test3 - All Data types ; High volume") {
    // Focused particularly on checking if all data types can be
    // extracted properly - including complex data types

    // check for row and column type
    val conn = getConn(locatorNetPort)
    val stmt = conn.createStatement()
//    SplitClusterDUnitTest.createTableUsingJDBC("test3tab1", "row", conn, stmt, Map.empty, true)

  }

  test("test4 - When partial cluster is not available/corrupted/deleted") {
    // check for row and column type

    // 1. what if one of diskstores is deleted - not available.
    // 2. what if some .crf files are missing
    // 3. what if some .drf files are missing
    // 4. what if some .krf files are missing

  }

*/

}

object RecoveryTestSuite {
  var snappyHome = ""

  def getJdbcConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    var url: String = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  // CWD will be assumed the same for all command which is $snappyHome
  def executeCommand(command: String): (String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream

    val teeOut = new TeeOutputStream(stdout, new BufferedOutputStream(stdoutStream))
    val teeErr = new TeeOutputStream(stderr, new BufferedOutputStream(stderrStream))

    val stdoutWriter = new PrintStream(teeOut, true)
    val stderrWriter = new PrintStream(teeErr, true)

    val code = Process(command, new File(s"$snappyHome")) !
//    scalastyle:off println
        ProcessLogger(stdoutWriter.println, stderrWriter.println)
//    scalastyle:on println

    var stdoutStr = stdoutStream.toString
    if (code != 0) {
      // add an exception to the output to force failure
      stdoutStr += s"\n***** Exit with Exception code = $code\n"
    }
    (stdoutStr, stderrStream.toString)
  }
}
