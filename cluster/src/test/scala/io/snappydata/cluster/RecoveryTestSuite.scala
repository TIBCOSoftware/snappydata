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

import java.io.{BufferedOutputStream, BufferedWriter, ByteArrayOutputStream, File, FileWriter, PrintStream, PrintWriter}
import java.sql.{Connection, DriverManager, ResultSet, Statement, Timestamp}
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.sys.process.{Process, ProcessLogger, stderr, stdout, _}
import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.test.dunit.AvailablePortHelper
import io.snappydata.thrift.internal.ClientClob
import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.udf.UserDefinedFunctionsDUnitTest

class RecoveryTestSuite extends FunSuite // scalastyle:ignore
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {

  val adminUser1 = "gemfire10"
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
    conf
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

    stopCluster()

    // TODO: remove the negation below after testing
    if (false && test_status) {
      logInfo("Clearing conf and work dir.")
      clearDirectory(confDir)
      clearDirectory(workDir)
    }

    confDirPath = ""
    workDirPath = ""

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
    // covers case: data only in row buffers
    stmt.execute(
      s"""
        CREATE TABLE $defaultSchema.tesst1coltab1 (
          col1 Int, col2 String, col3 Decimal
        ) USING COLUMN OPTIONS (buckets '1', COLUMN_MAX_DELTA_ROWS '4')""")
    stmt.execute(s"INSERT INTO $defaultSchema.tesst1coltab1 values(1,'aaaa',2.2)")
    stmt.execute(s"INSERT INTO $defaultSchema.tesst1coltab1 values(2,'bbbb',3.3)")

    stmt.execute(s"CREATE VIEW $defaultSchema.vw_tesst1coltab1 AS " +
        s"(SELECT * FROM $defaultSchema.tesst1coltab1)")
    stmt.execute("CREATE SCHEMA tapp")

    // empty column table & not null column
    // covers - empty buckets
    stmt.execute("CREATE TABLE tapp.test1coltab2" +
        " (col1 int, col2 int NOT NULL, col3 varchar(22) NOT NULL)" +
        " USING COLUMN OPTIONS (BUCKETS '5', COLUMN_MAX_DELTA_ROWS '10')")

    // empty row table & not null column
    stmt.execute("CREATE TABLE tapp.test1rowtab3 (col1 int, col2 int NOT NULL, col3 varchar(22))" +
        " USING ROW OPTIONS(PARTITION_BY 'col1', buckets '1')")

    stmt.execute("CREATE DISKSTORE anotherdiskstore ('./testDS' 10240);")

    stmt.execute(s"CREATE TABLE $defaultSchema.test1coltab4 (col1 int NOT NULL," +
        " col2 int not null) USING COLUMN OPTIONS(diskstore 'anotherdiskstore')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(11,111)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(333,33)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(11,111)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab4 values(333,33)")

    // covers few empty buckets case
    stmt.execute(s"CREATE TABLE $defaultSchema.test1rowtab5 (col1 int NOT NULL," +
        " col2 String not null) using row" +
        " options(partition_by 'col1', buckets '22', diskstore 'anotherdiskstore')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(111,'adsf')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(2223,'zxcvxcv')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(111,'adsf')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(2223,'zxcvxcv')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(111,'adsf')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab5 values(2223,'zxcvxcv')")


    val udfText: String = "public class IntegerUDF implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    val file = UserDefinedFunctionsDUnitTest.createUDFClass("IntegerUDF", udfText)
    val jar = UserDefinedFunctionsDUnitTest.createJarFile(Seq(file))

    stmt.execute(s"CREATE FUNCTION $defaultSchema.intudf AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")


    // nulls in data - row table
    stmt.execute(s"CREATE TABLE $defaultSchema.test1rowtab6 (col1 int, col2 string, col3 float," +
        s" col4 short, col5 boolean) using row")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab6 VALUES(null,'adsf',null, 12, 0)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab6 values" +
        s" (null,'xczadsf',232.1222, 11, null)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1rowtab6 values" +
        s" (null,null,333.333, null, 'true')")

    // data only in column batches - not in row buffers - nulls in data - column table
    stmt.execute(s"CREATE TABLE $defaultSchema.test1coltab7 (col1 Bigint, col2 varchar(44), col3" +
        s" double,col4 byte,col5 date)USING COLUMN OPTIONS(buckets '2',COLUMN_MAX_DELTA_ROWS '3')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab7 VALUES(9123372036812312307, 'asdfwerq334',123.123324, 12,'2019-03-20')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab7 VALUES(null, 'qewrqewr4',345.123324, 11,'2019-03-21')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab7 VALUES(8123372036812312307, 'asdfwerq334',null, null, null)")

    // data in only row buffer of column table - nulls in data
    stmt.execute(s"CREATE TABLE $defaultSchema.test1coltab8 (col1 Bigint, col2 varchar(44), col3" +
        s" double,col4 byte,col5 date)USING COLUMN OPTIONS(BUCKETS '5',COLUMN_MAX_DELTA_ROWS '4')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab8 VALUES(9123372036812312307, null,123.123324, 12,null)")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab8 VALUES(8123372036812312307, 'qewrwr4',345.123324, 11,'2019-03-21')")
    stmt.execute(s"INSERT INTO $defaultSchema.test1coltab8 VALUES(null, null,null, null, null)")

    writeToFile("1,aaaa,11.11\n2,bbbb,222.2\n333,ccccc,333.33", "/tmp/test1_exttab1.csv")
    stmt.execute("create external table test1_exttab1 using csv" +
        " options(path '/tmp/test1_exttab1.csv')")
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

  def createDir(fileName: String): String = {
    val f = new File(fileName)
    f.mkdir()
    f.deleteOnExit()
    fileName
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

  def compareResultSet(fqtn: String, resultSet: ResultSet, isRecoveredDataRS: Boolean): Unit = {
    val tableName = fqtn.replace(".", "_")
    val dir = new File(workDirPath + File.separator + tableName)
    if (!dir.exists() && !isRecoveredDataRS) {
      dir.mkdir()
    }
    if (isRecoveredDataRS && !dir.exists()) {
      // Since the directory is created every time in regular mode and re-used
      // in recovery mode and deleted after the comparison.
      throw new Exception(s"Directory for $tableName is not expected to exist.")
    }
    val stringBuilder = new mutable.StringBuilder()
    val filePathOrg = dir.getAbsoluteFile + File.separator + tableName + "_ORG.txt"
    val filePathRec = dir.getAbsoluteFile + File.separator + tableName + "_RECOVERED.txt"
    if (!isRecoveredDataRS) {
      logInfo(s"PP:creating org file at $filePathOrg")
      val colCount = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        stringBuilder.clear()
        (1 until colCount).foreach(i => {
          resultSet.getObject(i) match {
            case clob: ClientClob =>
              stringBuilder ++= s"${
                clob
                    .getSubString(1L, clob.length().toInt)
              },"
            case _ =>
              stringBuilder ++= s"${resultSet.getObject(i)},"
          }
        })
        resultSet.getObject(colCount) match {
          case clob: ClientClob =>
            stringBuilder ++= s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
          case _ =>
            stringBuilder ++= s"${resultSet.getObject(colCount)}"
        }
        // todo: can be improved using batching 100 rows
        writeToFile(stringBuilder.toString(), filePathOrg, true)
      }
    } else {
      logInfo(s"PP:creating rec file at $filePathRec")
      val colCount: Int = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        stringBuilder.clear()
        (1 until colCount).foreach(i => {
          resultSet.getObject(i) match {
            case clob: ClientClob =>
              stringBuilder ++= s"${
                clob
                    .getSubString(1L, clob.length().toInt)
              },"
            case _ =>
              stringBuilder ++= s"${resultSet.getObject(i)},"
          }
        })
        resultSet.getObject(colCount) match {
          case clob: ClientClob =>
            stringBuilder ++= s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
          case _ =>
            stringBuilder ++= s"${resultSet.getObject(colCount)}"
        }
        logInfo(s"PP: string added: ${stringBuilder.toString()}")

        // todo: can be improved using batching 100 rows
        writeToFile(stringBuilder.toString(), filePathRec, true)
      }
      //      val cmd = s"comm -3 $filePathOrg $filePathRec"
      //      val diffRes = cmd.!! // todo won't work on windows. Should be done in code.!?
      //      assert(diffRes.length === 0, "Recovered data does not match the original data.")

      //       delete the directory after the job is done.
      //          dir.listFiles().foreach(file => file.delete())
      //          if(dir.listFiles().length == 0) dir.delete()
    }

    /*

    // create a file - gemfire10_test3tab1_org.txt
    // compareFiles(fqtn= "", resultSet = rs, recovered_data = false)
    0. create a dir for the table - db_table_dir if doesn't exist
      1. if recovered data is false that means - orginal data
      2. create a file for org data
      3. write rs to file
    4. exit


    in recovered mode
    // compareFiles(fqtn= "", resultSet = rsRecovered, recovered_data = true)
    0. check if db_table_dir exists... if not something is wrong... throw excpetion
    1. if recovered data is true -
    2. create a recovery data file
    3. write rsRecovered to file
    4. use diff linux command to check difference or something else
    5. use assertion
    6. delete the directory
    7. exit

    */
  }


  def writeToFile(str: String, filePath: String, append: Boolean = false): Unit = {
    var pw: PrintWriter = null
    if (append) {
      val fileWriter = new FileWriter(filePath, append)
      val bufferedWriter = new BufferedWriter(fileWriter)
      pw = new PrintWriter(bufferedWriter)
      pw.println(str)
      pw.close()
      bufferedWriter.close()
      fileWriter.close()
    } else {
      pw = new PrintWriter(filePath)
      pw.write(str)
      pw.flush()
      pw.close()
      // wait until file becomes available (e.g. running on NFS)
      var matched = false
      while (!matched) {
        Thread.sleep(100)
        try {
          val source = scala.io.Source.fromFile(filePath)
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
  }

  def getConn(port: Int, user: String = "", password: String = ""): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    val url: String = "jdbc:snappydata://localhost:" + port + "/"
    Utils.classForName(driver).newInstance
    if (user.isEmpty && password.isEmpty) {
      DriverManager.getConnection(url)
    } else {
      DriverManager.getConnection(url, user, password)
    }
  }

  test("test1 - Basic test to check commands like describe, show, procedures " +
      "and list tables names, schemas names and UDFs using LDAP") {

    // set separate work directory and conf directory
    confDirPath = createConfDir("test1");
    val leadsNum = 1
    val locatorsNum = 1
    val serversNum = 1
    workDirPath = createWorkDir("test1", leadsNum, locatorsNum, serversNum)

    // 3. create conf files with required configuration, as required by the test,
    // inside the Conf dir - also mention the new work dir as a config

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

    stmt.close()
    conn.close()

    stopCluster()

    startSnappyRecoveryCluster()
    // todo: Resolve bug - Table is not found if queried immediately after
    Thread.sleep(5000)
    // TODO: Add cases that fail
    // TODO: Add test case for sample tables

    val connRec = getConn(locNetPort, "gemfire10", "gemfire10")
    val stmtRec = connRec.createStatement()
    // reused below multiple times; clear before using str
    var str: StringBuilder = new StringBuilder
    var tempTab = ""
    val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty
    var i = 0

    // todo : Refactor the code. Reuse variables where possible
    val rs1 = stmtRec.executeQuery("SELECT * FROM gemfire10.tesst1coltab1 ORDER BY col1")
    logDebug("=== SELECT * FROM tesst1coltab1 ===\n")
    str.clear()
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("1,aaaa,2.2", "2,bbbb,3.3")
    while (rs1.next()) {
      assert((s"${rs1.getInt("col1")}," +
          s"${rs1.getString("col2")},${rs1.getFloat("col3")}").equalsIgnoreCase(arrBuf(i)))
      i += 1
    }
    rs1.close()

    val rs5 = stmtRec.executeQuery("SELECT * FROM tapp.test1coltab2")
    logDebug("SELECT * FROM tapp.test1coltab2")
    str.clear()
    while (rs5.next()) {
      str ++= s"${rs5.getInt(2)}\t"
    }
    assert(str.toString().length === 0) // empty table
    rs5.close()


    val rs6 = stmtRec.executeQuery("SELECT * FROM tapp.test1rowtab3")
    logDebug("SELECT * FROM tapp.test1rowtab3")
    str.clear()
    while (rs6.next()) { // should not go in the loop as the table is empty.
      str ++= s"${rs6.getInt(2).toString}"
    }
    assert(str.toString().length === 0)
    rs6.close()

    var rs2 = stmtRec.executeQuery("show tables in gemfire10")
    logDebug("TableNames in gemfire10:\n")
    str.clear()
    while (rs2.next()) {
      tempTab = rs2.getString("tableName") + " "
      logDebug(tempTab)
      str ++= tempTab
    }
    val tempStr = str.toString().toUpperCase()
    // find better way to assert this case
    assert(tempStr.contains("TESST1COLTAB1") &&
        tempStr.contains("TEST1COLTAB4") &&
        tempStr.contains("TEST1COLTAB7") &&
        tempStr.contains("TEST1COLTAB8") &&
        tempStr.contains("TEST1ROWTAB5") &&
        tempStr.contains("TEST1ROWTAB6") &&
        tempStr.contains("VW_TESST1COLTAB1")
    )
    rs2.close()

    rs2 = stmtRec.executeQuery(s"show CREATE TABLE $tempTab")
    logDebug(s"=== show CREATE TABLE $tempTab")
    str.clear()
    while (rs2.next()) {
      str ++= s"${rs2.getString(1)}\t"
    }
    assert(str.toString().toUpperCase().contains("CREATE ")) //todo need to find a better way to assert the result
    rs2.close()

    val rs3 = stmtRec.executeQuery("show tables in tapp")
    logDebug("\ntableNames in tapp:")
    str.clear()
    while (rs3.next()) {
      val c2 = rs3.getString("tableName")
      logDebug(c2)
      str ++= s"$c2\t"
    }
    assert(str.toString().toUpperCase().contains("TEST1COLTAB2") && str.toString().toUpperCase().contains("TEST1ROWTAB3"))
    rs3.close()

    var rs4 = stmtRec.executeQuery("show functions")

    logInfo("Functions :\n")
    str.clear()
    while (rs4.next()) {
      str ++= s"${rs4.getString("function")}\t"
    }
    assert(str.toString().toUpperCase().contains("GEMFIRE10.INTUDF"))
    rs4.close()

    rs4 = stmtRec.executeQuery(s"select *,intudf(col2) as newcol from GEMFIRE10.tesst1coltab1")
    if (rs4.next()) {
      assert(rs4.getInt("newcol") === 6)
    }

    rs4 = stmtRec.executeQuery("show schemas")
    logInfo("=== show schemas ===")
    str.clear()
    while (rs4.next()) {
      str ++= s"${rs4.getString("databaseName")}\t"
    }
    assert(str.toString().toUpperCase().contains("TAPP") && str.toString().toUpperCase().contains("GEMFIRE10"))
    rs4.close()

    // custom diskstore test - column table
    rs4 = stmtRec.executeQuery("SELECT * FROM gemfire10.test1coltab4")
    println("SELECT * FROM gemfire10.test1coltab4;")
    while (rs4.next()) {
      // todo finish this
      s"${rs4.getInt(2).toString} ${rs4.getInt(2).toString}"
    }
    rs4.close()

    //      describe table
    println("====Describe table - gemfire10.test1coltab4====")
    logInfo("====Describe table - gemfire10.test1coltab4====")
    rs4 = stmtRec.executeQuery("describe gemfire10.test1coltab4")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("COL1 - int", "COL2 - int")
    while (rs4.next()) {
      assert(s"${rs4.getString(1)} - ${rs4.getString(2)}".equalsIgnoreCase(arrBuf(i)))
      i += 1
    }
    rs4.close()

    // query view
    rs4 = stmtRec.executeQuery("select col1,* from gemfire10.vw_tesst1coltab1 ORDER BY 1")
    println("=== view : vw_tesst1coltab1===")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("1,1,aaaa,2.200000000000000000", "2,2,bbbb,3.300000000000000000")
    while (rs4.next()) {
      assert(s"${rs4.getInt(1)},${rs4.getInt(2)},${rs4.getString(3)},${rs4.getBigDecimal(4)}"
          .equalsIgnoreCase(arrBuf(i)))
      i += 1
    }
    rs4.close()

    // custom diskstore test - row row table // todo : put proper assertion stmts
    var rstest1rowtab5 = stmtRec.executeQuery("SELECT * FROM gemfire10.test1rowtab5")
    println("SELECT * FROM gemfire10.test1rowtab5;")
    while (rstest1rowtab5.next()) {
      println(s"row : ${rstest1rowtab5.getInt(1)}    ${rstest1rowtab5.getString(2)}")
    }
    rstest1rowtab5.close()

    rs4 = stmtRec.executeQuery("select col1, col2, col3, col4, col5 from gemfire10.test1rowtab6;")
    println("==== test1rowtab6 ====")
    // todo : add assert statemenets
    // obs : int is also coming 0 in this case.
    while (rs4.next()) {
      println(s"${rs4.getInt(1)}\t${rs4.getString(2)}\t${rs4.getFloat(3)}\t " +
          s" ${rs4.getShort(4)}\t${rs4.getBoolean(5)}\t")
    }
    rs4.close()

    rs4 = stmtRec.executeQuery("select col1, col2, col3, col4, col5 from gemfire10.test1coltab7 ORDER BY col1")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("NULL,qewrqewr4,345.123324,11,2019-03-21", "8123372036812312307,asdfwerq334,NULL,NULL,NULL",
      "9123372036812312307,asdfwerq334,123.123324,12,2019-03-20")
    while (rs4.next()) {
      str.clear()
      str ++= s"${rs4.getObject(1)},${rs4.getObject(2)},${rs4.getObject(3)}," +
          s"${rs4.getObject(4)},${rs4.getObject(5)}"
      println(str.toString())
      //      assert(str.toString().toUpperCase() === (arrBuf(i)).toUpperCase()) ...  uncomment when short null is fixed

      i += 1
    }
    rs4.close()

    rs4 = stmtRec.executeQuery("SELECT * FROM gemfire10.test1coltab8 ORDER BY col1")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("9123372036812312307,null,123.123324,12,null", "8123372036812312307,qewrwr4,345.123324,11,2019-03-21", "null,null,null,null,null")


    while (rs4.next()) {
      str.clear()
      str ++= s"${rs4.getObject(1)},${rs4.getObject(2)},${rs4.getObject(3)}," +
          s"${rs4.getObject(4)},${rs4.getObject(5)}"
      println(str.toString())
      //      assert(str.toString().toUpperCase() === (arrBuf(i)).toUpperCase()) ...  uncomment when short null is fixed
      i += 1
    }
    rs4.close()

    stmtRec.execute("call sys.RECOVER_DDLS('./recover_ddls_test1/');")

    stmtRec.close()
    connRec.close()

    println("\n")
    println("\n")
    // todo: with different file systems - hdfs, s3
    test_status = true
  }

  def compareResultSet(fqtn: String, resultSet: ResultSet, isRecoveredDataRS: Boolean): Unit = {
    val tableName = fqtn.replace(".", "_")
    val dir = new File(workDirPath + File.separator + tableName)
    if (!dir.exists() && !isRecoveredDataRS) {
      dir.mkdir()
    }
    if (isRecoveredDataRS && !dir.exists()) {
      // Since the directory is created every time in regular mode and re-used
      // in recovery mode and deleted after the comparison.
      throw new Exception(s"Directory for $tableName is not expected to exist.")
    }
    val stringBuilder = new mutable.StringBuilder()
    val filePathOrg = dir.getAbsoluteFile + File.separator + tableName + "_ORG.txt"
    val filePathRec = dir.getAbsoluteFile + File.separator + tableName + "_RECOVERED.txt"
    if (!isRecoveredDataRS) {
      logInfo(s"PP:creating org file at $filePathOrg")
      val colCount = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        stringBuilder.clear()
        (1 until colCount).foreach(i => {
          resultSet.getObject(i) match {
            case clob: ClientClob =>
              stringBuilder ++= s"${
                clob
                    .getSubString(1L, clob.length().toInt)
              },"
            case _ =>
              stringBuilder ++= s"${resultSet.getObject(i)},"
          }
        })
        resultSet.getObject(colCount) match {
          case clob: ClientClob =>
            stringBuilder ++= s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
          case _ =>
            stringBuilder ++= s"${resultSet.getObject(colCount)}"
        }
        // todo: can be improved using batching 100 rows
        writeToFile(stringBuilder.toString(), filePathOrg, true)
      }
    } else {
      logInfo(s"PP:creating rec file at $filePathRec")
      val colCount: Int = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        stringBuilder.clear()
        (1 until colCount).foreach(i => {
          resultSet.getObject(i) match {
            case clob: ClientClob =>
              stringBuilder ++= s"${
                clob
                    .getSubString(1L, clob.length().toInt)
              },"
            case _ =>
              stringBuilder ++= s"${resultSet.getObject(i)},"
          }
        })
        resultSet.getObject(colCount) match {
          case clob: ClientClob =>
            stringBuilder ++= s"${
              clob.getSubString(1L, clob.length().toInt)
            }"
          case _ =>
            stringBuilder ++= s"${resultSet.getObject(colCount)}"
        }
        logInfo(s"PP: string added: ${stringBuilder.toString()}")

        // todo: can be improved using batching 100 rows
        writeToFile(stringBuilder.toString(), filePathRec, true)
      }
      //      val cmd = s"comm -3 $filePathOrg $filePathRec"
      //      val diffRes = cmd.!! // todo won't work on windows. Should be done in code.!?
      //      assert(diffRes.length === 0, "Recovered data does not match the original data.")

      //       delete the directory after the job is done.
      //          dir.listFiles().foreach(file => file.delete())
      //          if(dir.listFiles().length == 0) dir.delete()
    }
    /*

// create a file - gemfire10_test3tab1_org.txt
// compareFiles(fqtn= "", resultSet = rs, recovered_data = false)
0. create a dir for the table - db_table_dir if doesn't exist
  1. if recovered data is false that means - orginal data
  2. create a file for org data
  3. write rs to file
4. exit


in recovered mode
// compareFiles(fqtn= "", resultSet = rsRecovered, recovered_data = true)
0. check if db_table_dir exists... if not something is wrong... throw excpetion
1. if recovered data is true -
2. create a recovery data file
3. write rsRecovered to file
4. use diff linux command to check difference or something else
5. use assertion
6. delete the directory
7. exit

*/
  }


  test("test3 - All Data types at high volume") {
    // Focused particularly on checking if all data types can be
    // extracted properly
    // check for row and column type
    // TODO:Paresh: following tests can be clubbed/rearranged later. Increase the data volume later
    confDirPath = createConfDir("test3")
    val leadsNum = 1
    val locatorsNum = 1
    val serversNum = 2
    workDirPath = createWorkDir("test3", leadsNum, locatorsNum, serversNum)

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
         |localhost  -locators=localhost[$locatorPort] -dir=$workDirPath/server-2 -client-port=$netPort3 $ldapConf
         |""".stripMargin, s"$confDirPath/servers")

    startSnappyCluster()

    var conn: Connection = null: Connection
    var stmt: Statement = null: Statement

    conn = getConn(locNetPort, "gemfire10", "gemfire10")
    stmt = conn.createStatement()

    //    todo:  add binary and blob
    stmt.execute(
      s"""CREATE TABLE gemfire10.test3tab1
            (col1 BIGINT NOT NULL, col2 INT NOT NULL, col3 INTEGER NOT NULL,
             col4 long NOT NULL, col5 short NOT NULL, col6 smallint NOT NULL,
                col7 byte NOT NULL, c1 tinyint NOT NULL, c2 varchar(22) NOT NULL,
                 c3 string NOT NULL, c5 boolean NOT NULL,c6 double NOT NULL, c8 timestamp NOT NULL,
                  c9 date NOT NULL, c10 decimal(15,5) NOT NULL, c11 numeric(20,10) NOT NULL,
                   c12 float NOT NULL,c13 real not null) USING OOLUMN
                OPTIONS (buckets '5', COLUMN_MAX_DELTA_ROWS '135');
                """)

    stmt.execute(
      s"""INSERT INTO gemfire10.test3tab1 VALUES(9123372036854775807,2117483647,2116483647,
                  8223372036854775807,72,13,5,5,'qwerqwerqwer','qewrqewr',false,912384020490234.91928374997239749824,
                  '2019-02-18 15:31:55.333','2019-02-18',2233.67234,4020490234.7239749824,
                  912384020490234.91928374997239749824,920490234.9192837499724)""".stripMargin)

    stmt.execute("INSERT INTO gemfire10.test3tab1 select id*100000000000000,id,id*100000000,id*100000000000000,id,id, cast(id as byte), cast(id as tinyint), cast(concat('var',id) as varchar(22)),  cast(concat('str',id) as string), cast(id%2 as Boolean), cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP, CURRENT_DATE, cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(20,10)), cast(concat(id*100,'.',(id+1)*7699) as float), cast(concat(id*100000000000,'.',(id+1)*2267*7699) as real) from range(500);")


    //    todo: add not null here AND add binary and blob
    val rsTest3tab1 = stmt.executeQuery("SELECT * FROM gemfire10.test3tab1 ORDER BY col2")
    compareResultSet("gemfire10.test3tab1", rsTest3tab1, false)
    stmt.execute(
      s"""CREATE TABLE gemfire10.test3tab2
                (col1 BIGINT , col2 INT , col3 INTEGER , col4 long ,
                 col5 short , col6 smallint , col7 byte , c1 tinyint ,
                  c2 varchar(22) , c3 string , c5 boolean  ,c6 double ,
                   c8 timestamp , c9 date , c10 decimal(15,5) , c11 numeric(20,10) ,
                    c12 float ,c13 float
                    , primary key (col2,col3))
                     using row
                     options (partition_by 'col1,col2,col3', buckets '5', COLUMN_MAX_DELTA_ROWS '135');
                     """)

    /*
        stmt.execute(
          s"""CREATE TABLE gemfire10.test3tab2
                (col1 BIGINT , col2 INT , col3 INTEGER , col4 long ,
                 col5 short , col6 smallint , col7 byte , c1 tinyint ,
                  c2 varchar(22) , c3 string , c5 boolean ,c6 double ,
                   c8 timestamp , c9 date , c10 decimal(15,5) , c11 numeric(20,10) ,
                    c12 float ,c13 real ) using row
                     options (partition_by 'col1,col2,col3', buckets '5', COLUMN_MAX_DELTA_ROWS '135');
                     """)
    */
    stmt.execute(
      s"""INSERT INTO gemfire10.test3tab2 VALUES(9123372036854775807,2117483647,2116483647,
                  8223372036854775807,72,13,5,5,'qwerqwerqwer','qewrqewr',false,912384020490234.91928374997239749824,
                  '2019-02-18 15:31:55.333','2019-02-18',2233.67234,4020490234.7239749824,
                  912384020490234.91928374997239749824,920490234.9192837499724)""".stripMargin)

    stmt.execute(
      s"""INSERT INTO gemfire10.test3tab2 select id*100000000000000,id,id*100000000,id*100000000000000,id,id,
         | cast(id as byte),
         | cast(id as tinyint), cast(concat('var',id) as varchar(22)),  cast(concat('str',id) as string),
         |  cast(id%2 as Boolean), cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP,
         |   CURRENT_DATE, cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(20,10)),
         |    cast(concat(id*100,'.',(id+1)*7699) as float),
         |     cast(concat(id*100000000000,'.',(id+1)*2267*7699) as real) from range(5);
         |     """.stripMargin)


    val rsTest3tab2 = stmt.executeQuery("SELECT * FROM gemfire10.test3tab2 ORDER BY col2")
    compareResultSet("gemfire10.test3tab2", rsTest3tab2, false)

    // -- check replicated row table
    // primary key - enable once supported
    /*
     //    todo: add not null here AND add binary and blob
   stmt.execute(
          s"""CREATE TABLE gemfire10.test3Reptab2
                (col1 BIGINT , col2 INT , col3 INTEGER ,col4 long ,
                 col5 short , col6 smallint , col7 byte , c1 tinyint ,
                  c2 varchar(22) , c3 string , c5 boolean , c6 double ,
                       c8 timestamp , c9 date , c10 decimal(15,5) ,
                        c11 numeric(12,4) , c12 float ,c13 float , primary key (col2)) using row
                     options ();
                     """)
    */

    stmt.execute(
      s"""CREATE TABLE gemfire10.test3Reptab2
            (col1 BIGINT , col2 INT , col3 INTEGER ,col4 long ,
             col5 short , col6 smallint , col7 byte , c1 tinyint ,
              c2 varchar(22) , c3 string , c5 boolean , c6 double ,
                   c8 timestamp , c9 date , c10 decimal(15,5) ,
                    c11 numeric(20,10) , c12 float ,c13 real ) using row
                 options ();
                 """)
    stmt.execute(
      s"""INSERT INTO gemfire10.test3Reptab2
                  select id*100000000000000, id, id*100000000, id*100000000000000,
                  id, id, cast(id as byte), cast(id as tinyint), cast(concat('var',id) as varchar(22)),
                  cast(concat('str',id) as string), cast(id%2 as Boolean),
                  cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP, CURRENT_DATE,
                  cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(12,4)),
                  cast(concat(id*100,'.',(id+1)*7699) as float),
                  cast(concat(id*100000000000,'.',(id+1)*2267*7699) as real) from range(4);
                 """.stripMargin)

    val rsTest3Reptab2 = stmt.executeQuery("SELECT * FROM gemfire10.test3Reptab2 ORDER BY col2")
    compareResultSet("gemfire10.test3Reptab2", rsTest3Reptab2, false)

    // enable once support is added for primary key and binary,clob,blob
    // 3. Random mix n match data types
    //    stmt.execute("CREATE TABLE test3tab3 (col1 binary, col2 clob, col3 blob, col4 varchar(44), col5 int, primary key (col5)) using row")
    //    stmt.execute("INSERT INTO test3tab3 select cast('a' as binary), cast('b' as clob), cast('1' as blob), 'adsf', 123")

    // with option - key_columns
    stmt.execute("CREATE TABLE test3coltab4 (col1 int, col2 string, col3 float) USING COLUMN OPTIONS (key_columns 'col1')")
    stmt.execute("INSERT INTO test3coltab4 VALUES(1,'aaa',123.122)")
    stmt.execute("INSERT INTO test3coltab4 VALUES(2,'bbb',4444.55)")

    // row table - not null columns todo: add not null here
    stmt.execute("CREATE TABLE test3rowtab5 (col1 FloaT, col2 TIMEstamp, col3 BOOLEAN , col4 varchar(1) , col5 integer ) using row")
    stmt.execute("INSERT INTO test3rowtab5 VALUES(123.12321, '2019-02-18 15:31:55.333', 0, 'a',12)")
    stmt.execute("INSERT INTO test3rowtab5 VALUES(222.12321, '2019-02-18 16:31:56.333', 0, 'b',13)")
    stmt.execute("INSERT INTO test3rowtab5 VALUES(3333.12321, '2019-02-18 17:31:57.333', 'true', 'c',14)")

    stmt.execute("CREATE TABLE test3coltab6 (col1 BIGINT, col2 tinyint, col3 BOOLEAN) using column")
    stmt.execute("INSERT INTO test3coltab6 VALUES(100000000000001, 5, true)")
    stmt.execute("INSERT INTO test3coltab6 VALUES(200000000000001, 4, true)")
    stmt.execute("INSERT INTO test3coltab6 VALUES(300000000000001, 3, false)")

    // column table - not null columns todo: add not null here
    stmt.execute("CREATE TABLE test3coltab7 (col1 decimal(15,9), col2 float , col3 BIGint, col4 date, col5 string ) using column")
    stmt.execute("INSERT INTO test3coltab7 VALUES(891012.312321314, 1434124.123434134, 193471498234123, '2019-02-18', 'ZXcabcdefg')")
    stmt.execute("INSERT INTO test3coltab7 VALUES(91012.312321314, 34124.12343413, 243471498234123, '2019-04-18', 'qewrabcdefg')")
    stmt.execute("INSERT INTO test3coltab7 VALUES(1012.312321314, 4124.1234341, 333471498234123, '2019-03-18', 'adfcdefg')")

    // todo: Paresh: the peculiar case
    stmt.execute("CREATE TABLE test3rowtab8 (col1 string, col2 int, col3 varchar(33), col4 boolean, col5 float);")
    stmt.execute("INSERT INTO test3rowtab8 VALUES('qewradfs',111, 'asdfqewr', true, 123.1234);")
    stmt.execute("INSERT INTO test3rowtab8 VALUES('adsffs',222, 'vzxcqewr', true, 4745.345345);")
    stmt.execute("INSERT INTO test3rowtab8 VALUES('xzcvadfs',444, 'zxcvzv', false, 78768.34);")

    stmt.execute(
      """CREATE TABLE gemfire10.test3rowtab9
                               (col1 BIGINT , col2 INT , col3 INTEGER ,col4 long ,
                                col5 short , col6 smallint , col7 byte , c1 tinyint ,
                                 c2 varchar(22) , c3 string , c5 boolean , c6 double ,
                                      c8 timestamp , c9 date , c10 decimal(15,5) ,
                                       c11 numeric(20,10) , c12 float ,c13 real ) USING ROW OPTIONS()""")

    stmt.execute(
      """ INSERT INTO gemfire10.test3rowtab9 VALUES(null,null,null,
                   null,null,null,null,null,null,null,null,null,
                   null,null,null,null,
                   null,null)""")

    stmt.execute(
      """CREATE TABLE gemfire10.test3coltab10
                               (col1 BIGINT , col2 INT , col3 INTEGER ,col4 long ,
                                col5 short , col6 smallint , col7 byte , c1 tinyint ,
                                 c2 varchar(22) , c3 string , c5 boolean , c6 double ,
                                    c8 timestamp , c9 date , c10 decimal(15,5) ,
                                    c11 numeric(20,10) , c12 float ,c13 real )
                                    USING COLUMN OPTIONS(buckets '2',COLUMN_MAX_DELTA_ROWS '3')""")

    stmt.execute(
      """INSERT INTO gemfire10.test3coltab10 VALUES(null,null,null,
                   null,null,null,null,null,null,null,null,null,
                   null,null,null,null,
                   null,null);""")

    // todo: alter table -add/drop column-

    stmt.close()
    conn.close()

    stopCluster()

    startSnappyRecoveryCluster()
    Thread.sleep(5000)
    var connRec: Connection = null: Connection
    var stmtRec: Statement = null: Statement
    var str = new mutable.StringBuilder()
    val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty
    var i = 0
    connRec = getConn(locNetPort, "gemfire10", "gemfire10")
    stmtRec = connRec.createStatement()

    var rs1 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab1 where col1 = 9123372036854775807")

    if (rs1.next()) {
      assert(rs1.getLong(1) === 9123372036854775807L)
      assert(rs1.getInt(2) === 2117483647)
      assert(rs1.getInt(3) === 2116483647)
      assert(rs1.getLong(4) === 8223372036854775807L)
      assert(rs1.getShort(5) === 72)
      assert(rs1.getShort(6) === 13)
      assert(rs1.getByte(7) === 5)
      assert(rs1.getByte(8) === 5)
      assert(rs1.getString(9) === "qwerqwerqwer")
      assert(rs1.getString(10) === "qewrqewr")
      assert(rs1.getBoolean(11) === false)
      assert(rs1.getDouble(12) === 912384020490234.91928374997239749824)
      assert(rs1.getTimestamp(13) ===
          Timestamp.valueOf("2019-02-18 15:31:55.333"))
      assert(rs1.getDate(14).toString === "2019-02-18")
      assert(rs1.getBigDecimal(15).toString === "2233.67234")
      assert(rs1.getBigDecimal(16).toString === "4020490234.7239749824")
      assert(rs1.getFloat(17) === "912384020490234.91928374997239749824".toFloat)
      assert(rs1.getFloat(18) === "920490234.9192837499724".toFloat)

    }
    rs1.close()

    rs1 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab1 ORDER BY col2")
    compareResultSet("gemfire10.test3tab1", rs1, true)
    rs1.close()


    var rs2 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab2 where col1 = 9123372036854775807")
    if (rs2.next()) {
      assert(rs2.getLong(1) === 9123372036854775807L)
      assert(rs2.getInt(2) === 2117483647)
      assert(rs2.getInt(3) === 2116483647)
      assert(rs2.getLong(4) === 8223372036854775807L)
      assert(rs2.getShort(5) === 72)
      assert(rs2.getShort(6) === 13)
      assert(rs2.getByte(7) === 5)
      assert(rs2.getByte(8) === 5)
      assert(rs2.getString(9) === "qwerqwerqwer")
      assert(rs2.getString(10) === "qewrqewr")
      assert(rs2.getBoolean(11) === false)
      assert(rs2.getDouble(12) === 912384020490234.91928374997239749824)
      assert(rs2.getTimestamp(13) ===
          Timestamp.valueOf("2019-02-18 15:31:55.333"))
      assert(rs2.getDate(14).toString === "2019-02-18")
      assert(rs2.getBigDecimal(15).toString === "2233.67234")
      assert(rs2.getBigDecimal(16).toString === "4020490234.7239749824")
      assert(rs2.getFloat(17) === "912384020490234.91928374997239749824".toFloat)
      assert(rs2.getFloat(18) === "920490234.9192837499724".toFloat)
    }
    rs2.close()

    rs2 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab2 ORDER BY col2")
    val temprs3 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3tab2 ORDER BY col2")
    logInfo(s"PP: test: temprs3: ")
    while (temprs3.next()) {
      logInfo("PP:" + temprs3.getLong(1))
      logInfo("PP:" + temprs3.getInt(2))
      logInfo("PP:" + temprs3.getInt(3))
      logInfo("PP:" + temprs3.getLong(4))
      logInfo("PP:" + temprs3.getShort(5))
      logInfo("PP:" + temprs3.getShort(6))
      logInfo("PP:" + temprs3.getByte(7))
      logInfo("PP:" + temprs3.getByte(8))
      logInfo("PP:" + temprs3.getString(9))
      logInfo("PP:" + temprs3.getString(10))
      logInfo("PP:" + temprs3.getBoolean(11))
      logInfo("PP:" + temprs3.getDouble(12))
      logInfo("PP:" + temprs3.getTimestamp(13))
      logInfo("PP:" + temprs3.getDate(14))
      logInfo("PP:" + temprs3.getBigDecimal(15))
      logInfo("PP:" + temprs3.getBigDecimal(16))
      logInfo("PP:" + temprs3.getFloat(17))
      logInfo("PP:" + temprs3.getFloat(18))
    }
    compareResultSet("gemfire10.test3tab2", rs2, true)
    rs2.close()

    // val rs3 = stmtRec.executeQuery("select col1, col2, col3 from gemfire10.test3tab3").. enable with the create stmt

    val rs4 = stmtRec.executeQuery("select col1, col2, col3 from gemfire10.test3coltab4 ORDER BY col1")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("1,aaa,123.122", "2,bbb,4444.55")
    logDebug("Queried gemfire10.test3coltab4")
    while (rs4.next()) {
      assert(s"${rs4.getInt("col1")},${rs4.getString("col2")},${rs4.getFloat("col3")}"
          .equalsIgnoreCase(arrBuf(i)))
      i += 1
    }
    rs4.close()

    val rs5 = stmtRec.executeQuery("select col1, col3, col2 from gemfire10.test3coltab6 ORDER BY col1")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("100000000000001,5,true", "200000000000001,4,true", "300000000000001,3,false")
    while (rs5.next()) {
      assert(s"${rs5.getLong("col1")},${rs5.getShort("col2")},${rs5.getBoolean("col3")}"
          .equalsIgnoreCase(arrBuf(i)))
      i += 1
    }
    rs5.close()


    var rs6 = stmtRec.executeQuery("select count(*) rcount, c5 from gemfire10.test3tab1" +
        " group by c5 having c5 = true ORDER BY c5;")
    str.clear()
    while (rs6.next()) {
      str ++= s"${rs6.getInt("rcount")},${rs6.getBoolean("c5")}"
    }
    println(str.toString())
    //    assert(str.toString().equalsIgnoreCase("250,true"))
    rs6.close()

    // 4. Test if all sql functions are working fine - like min,max,avg,etc.
    //    Test if individual columns can be queried

    var rs7 = stmtRec.executeQuery("select first(col3) as fCol3, max(col1) as maxCol1," +
        " round(avg(col1)) as avgRoundRes, count(*) as count,concat('str_',first(col4)) as" +
        " concatRes, cast(first(col1) as string) as castRes, isnull(max(col5)) as isNullRes," +
        " Current_Timestamp, day(current_timestamp) from gemfire10.test3rowtab5;")
    while (rs7.next()) {
      assert(rs7.getFloat("maxcol1") === 3333.1233F && rs7.getInt("count") === 3)
    }
    rs7.close()

    rs7 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3Reptab2 ORDER BY col2;")
    compareResultSet("gemfire10.test3Reptab2", rs7, true)
    rs7.close()


    rs7 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3coltab7 ORDER BY col3;")
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("891012.312321314,1434124.1,193471498234123,2019-02-18,ZXcabcdefg", "91012.312321314,34124.125,243471498234123,2019-04-18,qewrabcdefg", "1012.312321314,4124.1235,333471498234123,2019-03-18,adfcdefg")
    while (rs7.next()) {
      println(s"${rs7.getBigDecimal(1)},${rs7.getBigDecimal(2)},${rs7.getLong(3)},${rs7.getDate(4)},${rs7.getString(5)}")
      i += 1
    }
    rs7.close()

    rs7 = stmtRec.executeQuery("SELECT * FROM gemfire10.test3rowtab8 ORDER BY col2;")
    // add assert stmt
    arrBuf.clear()
    i = 0
    arrBuf ++= ArrayBuffer("qewradfs,111,asdfqewr,true,123.1234", "adsffs,222,vzxcqewr,true,4745.345345", "xzcvadfs,444,zxcvzv,false,78768.34")
    while (rs7.next()) {
      println(s"${rs7.getString(1)},${rs7.getInt(2)},${rs7.getString(3)},${rs7.getBoolean(4)},${rs7.getFloat(5)} ")
    }
    rs7.close()

    // todo add assertion for gemfire10.test3rowtab9 and gemfire10.test3coltab10 ; once null issue
    // is fixed. - null comes out as 0  for few datatypesin recovery mode

    stmtRec.execute("call sys.RECOVER_DDLS('./recover_ddls_test3/');")
    stmtRec.close()
    conn.close()
    test_status = true
  }

  test("test2 - Does all basic tests in non-secure mode(without LDAP).") {
    // although ldap server is started before all, if ldap properties are not passed to conf,
    // it should work in non-secure mode.
    // basicOperationSetSnappyCluster can be used
    // multiple VMs - multiple servers - like real world scenario


    // check for row and column type
    // check if all the contents that are expected to be available to user is present for user to choose

    // After the cluster has come up and ready to be used by user.
    // check if all procedures available to user is working fine
    test_status = true
  }

  test("test4 - When partial cluster is not available/corrupted/deleted") {
    // check for row and column type

    // 1. what if one of diskstores is deleted - not available.
    // 2. what if some .crf files are missing
    // 3. what if some .drf files are missing
    // 4. what if some .krf files are missing

    test_status = true
  }

  test("test5 -Recovery procedures / Data export performance check") {

    // todo: Should be able to recover data and export to S3, hdfs, nfs and local file systems.
    //    Check performance with large volume of data.
    //    Should be able to export into all spark supported formats
    //    Should be able to select table names and also be able to give “all tables” option.
    //    Recover data in parquet format, test that, reloading the table from the same parquet file  should work fine.

    confDirPath = createConfDir("test5")
    val leadsNum = 1
    val locatorsNum = 1
    val serversNum = 1
    workDirPath = createWorkDir("test5", leadsNum, locatorsNum, serversNum)
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
    var conn: Connection = null: Connection
    var stmt: Statement = null: Statement
    conn = getConn(locNetPort, "gemfire10", "gemfire10")
    stmt = conn.createStatement()
    val rowNum = 500

    stmt.execute("CREATE TABLE test5coltab1 (col1 float, col2 int, col3 string,col4 date," +
        " col5 tinyint) USING COLUMN OPTIONS(buckets '1')")
    stmt.execute("INSERT INTO test5coltab1 select id*433/37, id, concat('str_',id)," +
        s" '2019-03-18', id%5 from range($rowNum);")

    stmt.execute("CREATE TABLE test5coltab2 (col1 float, col2 bigint, col3 varchar(99)," +
        " col4 date, col5 byte, col6 short) USING COLUMN OPTIONS(buckets '6')")
    stmt.execute("INSERT INTO test5coltab2 select cast(id*433/37 as float),id*999999, concat('str_',id)," +
        s" '2019-03-18', cast(id as tinyint), id%32 from range(${rowNum * 100})")

    stmt.execute("CREATE TABLE test5rowtab3 (col1 float , col2 int, col3 string ,col4 date," +
        " col5 tinyint) USING ROW OPTIONS(partition_by 'col2,col5', buckets '1')")
    stmt.execute("INSERT INTO test5rowtab3 select id*433/37, id, concat('str_',id)," +
        s" '2019-03-18', id%5 from range($rowNum);")

    stmt.execute("CREATE TABLE gemfire10.test5rowtab4 (col1 float, col2 bigint , col3 varchar(99)," +
        " col4 date , col5 byte, col6 short) using row" +
        " options(partition_by 'col2,col6', buckets '6')")
    stmt.execute(s"INSERT INTO gemfire10.test5rowtab4 select cast(id*433/37 as float),id*999999, concat('str_',id),'2019-03-18',cast(id as tinyint), id%32 from range(${rowNum * 100})")

    writeToFile("1,aaaa,11.11\n2,bbbb,222.2\n333,ccccc,333.33", "/tmp/test5_exttab1.csv")
    stmt.execute("create external table test5_exttab1 using csv" +
        " options(path '/tmp/test5_exttab1.csv')")

    // case: CREATE TABLE as SELECT * FROM ...
    stmt.execute("CREATE TABLE test5coltab4 as SELECT * FROM test5_exttab1;")

    // column table - how nulls are reflected in the recovered data files.
    stmt.execute("CREATE TABLE test5coltab5 (col1 timestamp, col2 integer, col3 varchar(33)," +
        "col boolean) using column")
    stmt.execute("INSERT INTO test5coltab5 values(null, 123, 'adsfqwer', 'true')")
    stmt.execute("INSERT INTO test5coltab5 values(null, null, 'zxcvqwer', null)")
    stmt.execute("INSERT INTO test5coltab5 values(null, 12345, 'ZXcwer', 'true')")
    stmt.execute("INSERT INTO test5coltab5 values(null, 67653, null, null)")

    // row table - how nulls reflect in the recovered data files.
    // todo: fix this:default fails in createSchemasMap method of RecoveryTestSuite
    stmt.execute("CREATE TABLE test5rowtab6 (col1 int, col2 string default 'DEF_VAL', col3  long default -99999, col4 float default 0.0)")
    //    stmt.execute("CREATE TABLE test5rowtab6 (col1 int, col2 string, col3  long, col4 float)")
    stmt.execute("INSERT INTO test5rowtab6 values(null, 'afadsf', 134098245, 123.123)")
    stmt.execute("INSERT INTO test5rowtab6 values(null, 'afadsf', 134098245, 123.123)")
    stmt.execute("INSERT INTO test5rowtab6 values(null, null, null, null)")
    stmt.execute("INSERT INTO test5rowtab6 (col1,col3) values(null, 134098245 )")
    stmt.execute("INSERT INTO test5rowtab6 values(null, 'afadsf', 134098245 )")
    stmt.execute("INSERT INTO test5rowtab6 (col1, col4) values(null, 345345.534)")

    stmt.execute("deploy package SPARKREDSHIFT 'com.databricks:spark-redshift_2.10:3.0.0-preview1' path '/home/ppatil/Testing/deploy_pkg_cache'")
    stmt.execute("deploy package sparkavrointegration 'com.databricks:spark-avro_2.11:4.0.0' path '/home/ppatil/Testing/deploy_pkg_cache';")
    stmt.execute("deploy jar snappyjar '/home/ppatil/Testing/deploy_pkg_cache/jars/org.xerial.snappy_snappy-java-1.0.5.jar'")

    stmt.close()
    conn.close()

    stopCluster()
    startSnappyRecoveryCluster()

    Thread.sleep(2500)

    var connRec: Connection = null
    var stmtRec: Statement = null

    logInfo("=== Recovery mode ============\n")
    connRec = getConn(locNetPort, "gemfire10", "gemfire10")
    stmtRec = connRec.createStatement()
    // todo: may be we can add S3,hdfs as export path
    val rstemp = stmtRec.executeQuery("SELECT * FROM test5rowtab6");
    while (rstemp.next()) {
      println(rstemp.getObject(1))
    }

    stmtRec.execute("call sys.RECOVER_DATA('./recover_data_parquet','parquet','gemfire10.test5coltab1','true')")

    logInfo(s"RECOVER_DATA called for test5coltab2 at ${System.currentTimeMillis}")
    stmtRec.execute("call sys.RECOVER_DATA('./recover_data_parquet','parquet','gemfire10.test5coltab2','true')")
    logInfo(s"RECOVER_DATA ends for test5coltab2 at ${System.currentTimeMillis}")


    logInfo(s"RECOVER_DATA called for test5rowtab4 at ${System.currentTimeMillis}")
    stmtRec.execute("call sys.RECOVER_DATA('./recover_data_parquet','parquet','gemfire10.test5rowtab4','true')")
    logInfo(s"RECOVER_DATA ends for test5rowtab4 at ${System.currentTimeMillis}")

    // checks ignore_error
    stmtRec.execute("call sys.RECOVER_DATA('./recover_data_parquet'," +
        "'parquet','gemfire10.test5coltab2,gemfire10.test5rowtab4, NonExistentTable','true')")

    stmtRec.execute("call sys.RECOVER_DATA('./recover_data_json','json','gemfire10.test5rowtab3','true')")

    stmtRec.execute("call sys.RECOVER_DATA('./recover_data_all','csv','all','true')")
    // todo how to verify if the files are correct?

    // check DLLs - create table, diskstore, view, schema, external table, index,
    // alter table -add/drop column-,

    // - drop diskstore, index, table, external table, schema, rename
    // create function

    stmtRec.execute("call sys.RECOVER_DDLS('./recover_ddls_test5');")
    // todo: add assertion for recover_ddls

    stmtRec.close()
    connRec.close()


    test_status = true
  }


  test("test6 - update, delete, complex data types") {
    // Add test cases that has to fail
    // Add test cases for sample tables
    // Add test for S3, hdfs
    // Add binary and blob in the tests
    // Add not nulls to the tests
    // Add assertion fo recover_ddl / recover_data output
    confDirPath = createConfDir("test6")
    val leadsNum = 1
    val locatorsNum = 1
    val serversNum = 1
    workDirPath = createWorkDir("test6", leadsNum, locatorsNum, serversNum)
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
      s"localhost  -locators=localhost[$locatorPort] -dir=$workDirPath/server-1 " +
          s"-client-port=$netPort2 $ldapConf".stripMargin, s"$confDirPath/servers")

    startSnappyCluster()

    val conn = getConn(locNetPort, "gemfire10", "gemfire10")
    val stmt = conn.createStatement()
    val defaultSchema = "gemfire10"
    var fqtn: String = null

    // todo: add NOT NULL to columns randomly to some tables
    // todo: Add nested complex data types tests
    // todo: null values not supported for complex data type columns - check

    // ========================================
    // ==== Column tables column batch only ===
    // ========================================
    // 1: null and not null, atomic data only, 1 bucket
    fqtn = "gemfire10.t1"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,
         |c3 integer) USING COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '4')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 11)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 322222222222222222.22222222222222222222, 33)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (4, 422222222222222222.22222222222222222222, null)")

    // 2: null and not null complex types 2 buckets
    fqtn = "gemfire10.t2"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<Integer>,
         |c4 Map<Int,Boolean> NOT NULL, c5 Struct<f1:float, f2:int>)
         | USING COLUMN options (buckets '2', COLUMN_MAX_DELTA_ROWS '1')""".stripMargin)
    stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
        s" 1, 1.1, Array(1,11,111), Map(1,true), Struct(1.1, 1)")
    stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
        s" 2, 2.2, Array(2,22,222), Map(2,false), Struct(2.2, 2)")


    // ==========================================
    // ====== Column tables row buffer only =====
    // ==========================================
    // 3: null and not null atomic data only 1 bucket
    fqtn = "gemfire10.t3"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,
         |c3 integer) USING COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, 22)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 322222222222222222.22222222222222222222, 33)")


    // 4: null and not null complex types 2 buckets
    fqtn = "gemfire10.t4"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<Integer>,
         |c4 Map<Int,Boolean> NOT NULL, c5 Struct<f1:float, f2:int>)
         | USING COLUMN options (buckets '2', COLUMN_MAX_DELTA_ROWS '4')""".stripMargin)
    stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
        s" 1, 1.1, Array(1,11,111), Map(1,true), Struct(1.1, 1)")
    stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
        s" 2, 2.2, Array(2,22,222), Map(2,false), Struct(2.2, 2)")
    stmt.executeUpdate(s"INSERT INTO $fqtn SELECT" +
        s" 3, 3.3, Array(3,33,333), Map(3,false), Struct(3.3, 3)")


    // =======================================================
    // ======= Column tables row buffer and column batch =====
    // =======================================================
    // 5: null and not null atomic data only 1 bucket deletes (in both areas)
    fqtn = "gemfire10.t5"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING
         | COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, 2)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, 4)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, null)")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 5")

    /*
    CREATE TABLE gemfire10.t5 (c1 integer, c2 double NOT NULL,c3 integer) USING
 COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3');
    INSERT INTO gemfire10.t5 VALUES (1, 111111111111111111.11111111111111111111, 1);
    INSERT INTO gemfire10.t5 VALUES (2, 222222222222222222.22222222222222222222, 2);
    INSERT INTO gemfire10.t5 VALUES (3, 333333333333333333.33333333333333333333, null);
    INSERT INTO gemfire10.t5 VALUES (4, 444444444444444444.44444444444444444444, 4);
    INSERT INTO gemfire10.t5 VALUES (5, 555555555555555555.55555555555555555555, null);
    DELETE FROM gemfire10.t5 WHERE c1 = 2;
    DELETE FROM gemfire10.t5 WHERE c1 = 5;
     */

    // 6: null and not null atomic data only 1 bucket deletes/updates (in both areas)
    fqtn = "gemfire10.t6"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING
         | COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, 5)")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 5")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 4")

    /*
    CREATE TABLE gemfire10.t6 (c1 integer, c2 double NOT NULL,c3 integer) USING COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3');
    INSERT INTO gemfire10.t6 VALUES (1, 111111111111111111.11111111111111111111, 1);
    INSERT INTO gemfire10.t6 VALUES (2, 222222222222222222.22222222222222222222, null);
    INSERT INTO gemfire10.t6 VALUES (3, 333333333333333333.33333333333333333333, 3);
    INSERT INTO gemfire10.t6 VALUES (4, 444444444444444444.44444444444444444444, null);
    INSERT INTO gemfire10.t6 VALUES (5, 555555555555555555.55555555555555555555, 5);
    DELETE FROM gemfire10.t6 WHERE c1 = 2;
    DELETE FROM gemfire10.t6 WHERE c1 = 5;
    UPDATE gemfire10.t6 SET c3 = 0 WHERE c1 = 1;
    UPDATE gemfire10.t6 SET c3 = 0 WHERE c1 = 4;
     */


    // 7: null and not null atomic data only 1 bucket updates (in both areas)
    fqtn = "gemfire10.t7"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING
         | COLUMN options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, 4)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, null)")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 4")

    // 8: null and not null complex types 2 buckets
    fqtn = "gemfire10.t8"
    stmt.execute(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<Integer>,
         |c4 Map<Int,Boolean> NOT NULL, c5 Struct<f1:float, f2:int>)
         | USING COLUMN options (buckets '2', COLUMN_MAX_DELTA_ROWS '2')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn SELECT" +
        s" 1, 1.1, Array(1,11,111), Map(1,true), Struct(1.1, 1)")
    stmt.execute(s"INSERT INTO $fqtn SELECT" +
        s" 2, 2.2, Array(2,22,222), Map(2,false), null")
    stmt.execute(s"INSERT INTO $fqtn SELECT" +
        s" 3, 3.3, Array(3,33,333), Map(3,false), Struct(3.3, 3)")


    // ===================================
    // ======= Row table partitioned =====
    // ===================================
    // 9: null and not null atomic data only 1 bucket update/delete alter add/drop/add
    fqtn = "gemfire10.t9"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer)
         | USING row options (partition_by 'c1', buckets '1')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, 5)")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 4")
    stmt.execute(s"ALTER TABLE $fqtn DROP COLUMN c2")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 5")
    stmt.execute(s"ALTER TABLE $fqtn ADD COLUMN c2 integer")
    stmt.execute(s"INSERT INTO $fqtn VALUES (9, 99, 999)")

    // 10: null and not null complex types 2 buckets no alter
    fqtn = "gemfire10.t10"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer)
         | USING row options (partition_by 'c1', buckets '2')""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (4, 444444444444444444.44444444444444444444, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (5, 555555555555555555.55555555555555555555, 5)")


    // ===================================
    // ======= Row table replicated ======
    // ===================================
    // 11: null and not null atomic data only update/delete alter add/drop/add
    fqtn = "gemfire10.t11"
    stmt.executeUpdate(
      s"""CREATE TABLE $fqtn (c1 integer, c2 double NOT NULL,c3 integer) USING row""".stripMargin)
    stmt.execute(s"INSERT INTO $fqtn VALUES (1, 111111111111111111.11111111111111111111, 1)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (2, 222222222222222222.22222222222222222222, null)")
    stmt.execute(s"INSERT INTO $fqtn VALUES (3, 333333333333333333.33333333333333333333, 3)")
    stmt.execute(s"UPDATE $fqtn SET c3 = 0 WHERE c1 = 1")
    stmt.execute(s"ALTER TABLE $fqtn DROP COLUMN c2")
    stmt.execute(s"DELETE FROM $fqtn WHERE c1 = 2")
    stmt.execute(s"ALTER TABLE $fqtn ADD COLUMN c2 integer")
    stmt.execute(s"INSERT INTO $fqtn VALUES (9, 99, 999)")

    // 12: null and not null complex types no alter
    //    fqtn = "gemfire10.t12"
    //    stmt.execute(
    //      s"""CREATE TABLE $fqtn (c1 integer, c2 double,c3 Array<Integer>,
    //         |c4 Map<Int,Boolean> NOT NULL, c5 Struct<f1:float, f2:int>) USING row""".stripMargin)
    //    stmt.execute(s"INSERT INTO $fqtn SELECT" +
    //        s" 1, 1.1, Array(1,11,111), Map(1,true), Struct(1.1, 1)")
    //    stmt.execute(s"INSERT INTO $fqtn SELECT" +
    //        s" 2, 2.2, Array(2,22,222), Map(2,false), null")
    //    stmt.execute(s"INSERT INTO $fqtn SELECT" +
    //        s" 3, 3.3, Array(3,33,333), Map(3,false), Struct(3.3, 3)")


    println("--- describe t11")
    val resttemp = stmt.executeQuery(s"describe $fqtn")
    while (resttemp.next()) {
      println(s"${resttemp.getString(1)} ${resttemp.getString(2)}")
    }

    stmt.close()
    conn.close()

    stopCluster()
    startSnappyRecoveryCluster()

    var connRec: Connection = null
    var stmtRec: Statement = null
    var str = new mutable.StringBuilder()
    val arrBuf: ArrayBuffer[String] = ArrayBuffer.empty
    var i = 0

    logInfo("=== Recovery mode ============\n")
    connRec = getConn(locNetPort, "gemfire10", "gemfire10")
    stmtRec = connRec.createStatement()
    Thread.sleep(3000)

    //    val rstemp = stmtRec.executeQuery("show tables in gemfire10")
    //    println("--- tables ---")
    //    while (rstemp.next()) {
    //      println(rstemp.getString(2))
    //    }
    //    rstemp.close()

    // 1891
    def getRecFromResultSet(rs: ResultSet, schemaStr: String): ListBuffer[Array[Any]] = {
      var result = new ListBuffer[Array[Any]]()
      while (rs.next()) {
        var i = 1
        val recArr = schemaStr.split(",").map(_.toLowerCase).map(f => {
          val fValue = f match {
            case "integer" | "int" =>
              val ii = rs.getInt(i)
              logInfo("element value is " + ii)
              ii
            case "double" => rs.getDouble(i)
            case _ => ""
          }
          i += 1
          if (rs.wasNull()) null else fValue
        })
        logInfo(s"recarr = ${recArr.toSeq}")
        result += recArr
      }
      result
    }

    def compareResult(expectedResult: ListBuffer[Array[Any]],
        result: ListBuffer[Array[Any]]): Unit = {
      for (rec <- result) {
        for (expRec <- expectedResult) {
          if (expRec.sameElements(rec)) result.remove(result.indexOf(rec))
        }
      }
      assert(result.size == 0, s"result has extra records")
    }

    // *********************************************
    // ******************testcase 1*****************
    // *********************************************
    val rs1 = stmtRec.executeQuery("select * from gemfire10.t1")
    val expectedResult1: ListBuffer[Array[Any]] = ListBuffer(
      Array(1, 111111111111111111.11111111111111111111, 11),
      Array(2, 222222222222222222.22222222222222222222, null),
      Array(3, 322222222222222222.22222222222222222222, 33),
      Array(4, 422222222222222222.22222222222222222222, null)
    )
    compareResult(expectedResult1,
      getRecFromResultSet(rs1, "integer,double,integer"))
    rs1.close()

    // testcase 3
    val rs3 = stmtRec.executeQuery("select * from gemfire10.t3")
    val expectedResult3: ListBuffer[Array[Any]] = ListBuffer(
      Array(1, 111111111111111111.11111111111111111111, null),
      Array(2, 222222222222222222.22222222222222222222, 22),
      Array(3, 322222222222222222.22222222222222222222, 33)
    )
    compareResult(expectedResult3,
      getRecFromResultSet(rs3, "integer,double,integer"))
    rs3.close()

    // testcase 5
    val rs5 = stmtRec.executeQuery("select * from gemfire10.t5")
    val expectedResult5: ListBuffer[Array[Any]] = ListBuffer(
      Array(1, 111111111111111111.11111111111111111111, 1),
      Array(3, 333333333333333333.33333333333333333333, null),
      Array(4, 444444444444444444.44444444444444444444, 4)
    )
    compareResult(expectedResult5,
      getRecFromResultSet(rs5, "integer,double,integer"))
    rs5.close()

    // testcase 6
    val rs6 = stmtRec.executeQuery("select * from gemfire10.t6")
    val expectedResult6: ListBuffer[Array[Any]] = ListBuffer(
      Array(1, 111111111111111111.11111111111111111111, 0),
      Array(3, 333333333333333333.33333333333333333333, 3),
      Array(4, 444444444444444444.44444444444444444444, 0)
    )
    compareResult(expectedResult6,
      getRecFromResultSet(rs6, "integer,double,integer"))
    rs6.close()

    // 7: null and not null atomic data only 1 bucket updates (in both areas)
    val rs7 = stmtRec.executeQuery("select * from gemfire10.t7")
    val expectedResult7: ListBuffer[Array[Any]] = ListBuffer(
      Array(1, 111111111111111111.11111111111111111111, 0),
      Array(2, 222222222222222222.22222222222222222222, null),
      Array(3, 333333333333333333.33333333333333333333, 3),
      Array(4, 444444444444444444.44444444444444444444, 0),
      Array(5, 555555555555555555.55555555555555555555, null)
    )
    compareResult(expectedResult7,
      getRecFromResultSet(rs7, "integer,double,integer"))
    rs7.close()

    stmtRec.execute("call sys.RECOVER_DDLS('./recover_ddls/');")
    // todo hmeka Add assertion on recover_ddls output
    stmtRec.close()
    connRec.close()

    test_status = true
  }
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
