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
import java.sql.{Connection, DriverManager, SQLException, Statement, Timestamp}
import java.util.Properties

import scala.sys.process.{Process, ProcessLogger, stderr, stdout}
import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils

class RecoveryTestSuite extends FunSuite // scalastyle:ignore
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {

  val adminUser1 = "gemfire10"
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


    // 1. Stop the cluster - before clearing conf - as it is required by stop script
    stopCluster()

    // 2. check if the test is successful (test_status flag)- then clear the conf dir and work dir
    // 3. if the test is not successful don't delete
    // clear confDirPath and workDirPath variables

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
    // 1. create new schema
    // 2. create tables in new schema - both row and column
    // 3. create UDF

    stmt.execute(
      s"""
        CREATE TABLE $defaultSchema.tesst1coltab1 (
          col1 Int, col2 String, col3 Decimal
        ) USING column options (buckets '1', COLUMN_MAX_DELTA_ROWS '3')""")

    stmt.execute(s"insert into $defaultSchema.tesst1coltab1 values(1,'aaaa',2.2)")
    stmt.execute(s"insert into $defaultSchema.tesst1coltab1 values(2,'bbbb',3.3)")

    stmt.execute(s"create view $defaultSchema.vw_tesst1coltab1 AS " +
        s"(select * from $defaultSchema.tesst1coltab1)")
    stmt.execute("create schema tapp")

    // make it different from tesst1coltab1
    stmt.execute("create table tapp.test1coltab2 (col1 int, col2 int, col3 varchar(22))" +
        " using column options (BUCKETS '5', COLUMN_MAX_DELTA_ROWS '10')")

    stmt.execute("insert into tapp.test1coltab2 values(22,22,'adsf')")

        stmt.execute("create table tapp.test1rowtab3 (col1 int, col2 int, col3 varchar(22))" +
            " using row options(PARTITION_BY 'col1', buckets '1')")
        stmt.execute("insert into tapp.test1rowtab3 values(22,22,'adsf')")

    // -- enable after adding support
    /*
        stmt.execute("create diskstore anotherdiskstore")
        stmt.execute("create table tapp.test1coltab4 (col1 int not null," +
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

        stmt.executeQuery(s"select *,intudf(col2) as newcol from $defaultSchema.tesst1coltab1")
    */
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


  test("test1 - Basic test to check commands like describe, show, procedures " +
      "and list tables names, schemas names and UDFs using LDAP") {

    println("Recovery Test Dir: " + System.getProperty("RECOVERY_TEST_DIR")
        + "\n" + recovery_mode_dir)

    println("PP:RecoveryModeTestSuite: inside test1:\n test status flag: " + test_status)

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
        val rs = stmt.executeQuery("select * from gemfire10.tesst1coltab1")
        println("\n")
        println("=== select * from gemfire10.tesst1coltab1 === \n")

        while (rs.next()) {
          println(rs.getString("col2"))
        }
        rs.close()

    println("\n")
    println("\n")
    stmt.close()
    conn.close()


    stopCluster()

    startSnappyRecoveryCluster()

    // 1. do a show query and check if catalog is populated properly

    val connRec = getConn(locNetPort, "gemfire10", "gemfire10")
    val stmtRec = connRec.createStatement()

    // enable this once data can be read from file.
    val rs1 = stmtRec.executeQuery("select * from gemfire10.tesst1coltab1")
    println("=== Recovery mode ============\n")
    println("=== select * from tesst1coltab1 ===\n")

    while (rs1.next()) {
      println(rs1.getString("col2"))
    }
    rs1.close()

    val rs5 = stmtRec.executeQuery("select * from tapp.test1coltab2")
    println("=== Recovery mode ========")
    println("select * from tapp.test1coltab2")

    while (rs5.next()) {
      println(rs5.getInt(2).toString)
    }
    rs5.close()
        val rs6 = stmtRec.executeQuery("select * from tapp.test1rowtab3")
        println("=== Recovery mode ========")
        println("select * from tapp.test1rowtab3")

        while(rs6.next()) {
          println(rs6.getInt(2).toString)
        }
        rs6.close()

    try {
      val rs2 = stmtRec.executeQuery("show tables in gemfire10")
      println("tableNames in gemfire10:\n")
      while (rs2.next()) {
        val c2 = rs2.getString("tableName")
        println(c2)
      }
      rs2.close()

      val rs3 = stmtRec.executeQuery("show tables in tapp")
      println("tableNames in tapp:\n")
      while (rs3.next()) {
        val c2 = rs3.getString("tableName")
        println(c2)
      }
      rs3.close()

      val rs4 = stmtRec.executeQuery("show functions")
      println("Functions :\n")
      while (rs4.next()) {
        println(rs4.getString("function"))
      }
      rs4.close()

    } finally {
      stmtRec.close()
      connRec.close()
    }

    println("\n")
    println("\n")

    // check if all the contents that are expected to be available
    // to user is present for user to choose.

    // After the cluster has come up and ready to be used by user.
    // check if all procedures available to user is working fine

    // at the end ==== set the test_status flag to true
    test_status = true
  }

  test("test3 - All Data types at high volume") {
    // Focused particularly on checking if all data types can be
    // extracted properly - including complex data types
    // check for row and column type
    // TODO:Paresh: following tests can be clubbed/rearranged later. Increase the data volume later

    confDirPath = createConfDir("test3")
    val leadsNum = 1
    val locatorsNum = 1
    val serversNum = 1
    workDirPath = createWorkDir("test3", leadsNum, locatorsNum, serversNum)

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

    var conn: Connection = null: Connection
    var stmt: Statement = null: Statement

    try {
      conn = getConn(locNetPort, "gemfire10", "gemfire10")
      stmt = conn.createStatement()

      // 1. Checks all data types but [short, smallint, byte, tinyint, boolean] for column tables - for delta rows.
      // this can be used for high volume testing
      stmt.execute(
        s"""create table test3tab1(col1 BIGINT, col2 INT, col3 INTEGER, col4 long, col5 short, col6 smallint,
                col7 byte, c1 tinyint, c2 varchar(22), c3 string, c5 boolean ,c6 double, c8 timestamp, c9 date,
                c10 decimal(15,5), c11 numeric(20,10), c12 real,c13 float) using column
                options (buckets '5', COLUMN_MAX_DELTA_ROWS '3');
                """)

            stmt.execute(
              s"""insert into test3tab1 values (9123372036854775807,2117483647,2116483647,
                  8223372036854775807,72,13,5,5,'qwerqwerqwer','qewrqewr',false,912384020490234.91928374997239749824,
                  '2019-02-18 15:31:55.333','2019-02-18',2233.67234,4020490234.7239749824,
                  912384020490234.91928374997239749824,920490234.9192837499724)""".stripMargin)

      stmt.execute("insert into test3tab1 select id*100000000000000,id*1000000,id*100000000,id*100000000000000,id,id, cast(id as byte), cast(id as tinyint), cast(concat('var',id) as varchar(22)),  cast(concat('str',id) as string), cast(id%2 as Boolean), cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP, CURRENT_DATE, cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(20,10)), cast(concat(id*100,'.',(id+1)*7699) as real), cast(concat(id*100000000000,'.',(id+1)*2267*7699) as float) from range(500);")



      /*
            prepStmt1.setLong(1, 12341234)
            prepStmt1.setInt(2, 1324)
            prepStmt1.setInt(3, 435234)
            prepStmt1.setLong(4, 1234134123)
            prepStmt1.setShort(5, 123)
            prepStmt1.setShort(6, 1231)
            prepStmt1.setByte(7, 8.toByte)
            prepStmt1.setByte(8, 3.toByte)
            prepStmt1.setString(9, "qerew")
            prepStmt1.setString(10, "qewrqewr")
            prepStmt1.setBoolean(11, true)
            prepStmt1.setDouble(12, 11.11)
            prepStmt1.setTimestamp(13, new java.sql.Timestamp(System.currentTimeMillis()))
            prepStmt1.setDate(14, new java.sql.Date(System.currentTimeMillis()))
            prepStmt1.setBigDecimal(15, new java.math.BigDecimal(1212.3123))
            prepStmt1.setBigDecimal(16, new java.math.BigDecimal(123.123))
            prepStmt1.setBigDecimal(17, new java.math.BigDecimal(213.123))
            prepStmt1.setFloat(18, 213.12F)

            prepStmt1.executeUpdate()
      */

      //    2. Checks all data types  but [short, smallint, byte, tinyint, boolean] for row table
      stmt.execute(
        s"""create table test3tab2(col1 BIGINT, col2 INT, col3 INTEGER, col4 long, col5 short, col6 smallint,
                 col7 byte, c1 tinyint, c2 varchar(22), c3 string, c5 boolean ,c6 double, c8 timestamp, c9 date,
                 c10 decimal(15,5), c11 numeric(20,10), c12 real,c13 float) using row
                 options (partition_by 'col1,col2,col3,c13', buckets '5', COLUMN_MAX_DELTA_ROWS '3');
                 """)

      stmt.execute(
        s"""insert into test3tab2 values (9123372036854775807,2117483647,2116483647,
                  8223372036854775807,72,13,5,5,'qwerqwerqwer','qewrqewr',false,912384020490234.91928374997239749824,
                  '2019-02-18 15:31:55.333','2019-02-18',2233.67234,4020490234.7239749824,
                  912384020490234.91928374997239749824,920490234.9192837499724)""".stripMargin)

      stmt.execute(
        s"""insert into test3tab2 select id*100000000000000,id*1000000,id*100000000,id*100000000000000,id,id, cast(id as byte), cast(id as tinyint), cast(concat('var',id) as varchar(22)),  cast(concat('str',id) as string), cast(id%2 as Boolean), cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP, CURRENT_DATE, cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(20,10)), cast(concat(id*100,'.',(id+1)*7699) as real), cast(concat(id*100000000000,'.',(id+1)*2267*7699) as float) from range(500);""".stripMargin)

      // enable once issue of "no buckets mentioned" is resolved
 /*     // -- check replicated row table
      stmt.execute(
        s"""create table test3tab2(col1 BIGINT, col2 INT, col3 INTEGER,col4 long, col5 short,
             col6 smallint, col7 byte, c1 tinyint, c2 varchar(22), c3 string, c5 boolean ,c6 double,
              c8 timestamp, c9 date, c10 decimal(15,5), c11 numeric(12,4), c12 real,c13 float) using row
            options ();
            """)

      stmt.execute(
        s"""insert into test3tab2
             select id*100000000000000, id*1000000, id*100000000, id*100000000000000,
             id, id, cast(id as byte), cast(id as tinyint), cast(concat('var',id) as varchar(22)),
             cast(concat('str',id) as string), cast(id%2 as Boolean),
             cast((id*701*7699 + id*1342341*2267)/2 as double), CURRENT_TIMESTAMP, CURRENT_DATE,
             cast(id*241/11 as Decimal(15,5)), cast(id*701/11 as Numeric(12,4)),
             cast(concat(id*100,'.',(id+1)*7699) as real),
             cast(concat(id*100000000000,'.',(id+1)*2267*7699) as float) from range(500);
            """.stripMargin)
*/

// enable once support is added
      // 3. Random mix n match data types
      //            stmt.execute("create table test3tab3 (col1 binary, col2 clob, col3 blob) using row")
      //            stmt.execute("insert into test3tab3 select cast('a' as binary), cast('b' as clob), cast('1' as blob)")


                  stmt.execute("create table test3tab4 (col1 int, col2 string, col3 float) using column")
                  stmt.execute("insert into test3tab4 values (1,'aaa',123.122)")


      // enable once buckets issue is resolved
//      stmt.execute("create table test3tab5 (col1 FloaT, col2 TIMEstamp, col3 BOOLEAN, col4 varchar(1)) using row")
//      stmt.execute("insert into test3tab5 values (123.12321, '2019-02-18 15:31:55.333', 0, 'a')")

      //            stmt.execute("create table test3tab6 (col1 FloaT, col2 TIMEstamp, col3 BOOLEAN, col4 varchar(1)) using column")
      //            stmt.execute("insert into test3tab6 values (213.23424, '2019-02-18 15:31:55.333', 23, 'b')")

      //            stmt.execute("create table test3tab7 (col1 decimal(15,9), col2 real, col3 BIGint, col4 date, col5 string) using column")
      //            stmt.execute("insert into test3tab7 values (891012.312321314, 1434124.123434134, 193471498234123, '2019-02-18', 'abcdefg')")


      // check replicated mode...
      // test describe
      // test row table with
      /*
       * empty  table
       * some rows
       */

      // test column table with
      /*
       * empty  table
       * some rows in row buffer and no rows in col buffer
       * some rows in row buffer and some rows in col buffer
       * no rows in row buffer and col buffer is full
       * with multiple buckets in each case
       */

    }
    finally {
      stmt.close()
      conn.close()
    }

// TODO: ================= ** Move this after the startup in recovery mode ** ======================== //

    var connRec: Connection = null: Connection
    var stmtRec: Statement = null: Statement


    stopCluster()

    startSnappyRecoveryCluster()
            try {
              logInfo("=== Recovery mode ============\n")
              connRec = getConn(locNetPort, "gemfire10", "gemfire10")
              stmtRec = connRec.createStatement()

              val rs1 = stmtRec.executeQuery("select * from gemfire10.test3tab1 where col1 = 9123372036854775807")
              logInfo("=== select col1 from test3tab1 ===\n")

              if (rs1.next()) {

                // TODO: delete later
                for (i <- 1 to rs1.getMetaData.getColumnCount) {
                  println(s"col$i         class = ${rs1.getMetaData.getColumnClassName(i)}" +
                      s" : type_int = ${rs1.getMetaData.getColumnType(i)} " +
                      s"type_db_specific_name = ${rs1.getMetaData.getColumnTypeName(i)}")
                }

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

                      val rs2 = stmtRec.executeQuery("select * from gemfire10.test3tab2 where col1 = 9123372036854775807")
                      logInfo("=== Recovery mode ============\n")
                      logInfo("=== select * from test3tab2 ===\n")
                      if (rs2.next()) {
                        println(rs2.getMetaData.getTableName(1))
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
                      // to verify match the number of rows
                      // also verify if select specific columns are working and sql functions
                      // on the columns are working.
                      // to verify table with all the data types - match the output

                //      val rs3 = stmtRec.executeQuery("select col1, col2, col3 from gemfire10.test3tab3")


                //      val rs4 = stmtRec.executeQuery("select col1, col2, col3 from gemfire10.test3tab4")
                //      val rs5 = stmtRec.executeQuery("select col3, col2, col4, cast(col1 as Integer) from gemfire10.test3tab5")


      connRec = getConn(locNetPort, "gemfire10", "gemfire10")
      stmtRec = connRec.createStatement()


      var rs5 = stmtRec.executeQuery("select count(*) rcount from gemfire10.test3tab1;")
      rs5.next()

      println(rs5.getInt("rcount"))
      rs5.close()


      rs5 = stmtRec.executeQuery("select count(*) rcount from gemfire10.test3tab2;")
      rs5.next()

      println(rs5.getInt("rcount"))
      rs5.close()

      /*
      var rs5 = stmtRec.executeQuery("select first(col3) as fcol3, max(col1) as mcol1 from gemfire10.test3tab5")
      while(rs5.next()) {
        val col3 = rs5.getBoolean("fcol3")
        val col1 = rs5.getFloat("mcol1")
        println((col1, col3))
      }
      rs5.close()
      */

    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      stmtRec.close()
      conn.close()
    }

    //    SplitClusterDUnitTest.createTableUsingJDBC("test3tab1", "row", conn, stmt, Map.empty, true)
    test_status = true
  }

/*
  test("test2 - Does all basic tests in non-secure mode(without LDAP).") {
    // although ldap server is started before all, if ldap properties are not passed to conf,
    // it should work in non-secure mode.
    // basicOperationSetSnappyCluster can be used
    // multiple VMs - multiple servers - like real world scenario


    // check for row and column type
    // check if all the contents that are expected to be available to user is present for user to choose

    // After the cluster has come up and ready to be used by user.
    // check if all procedures available to user is working fine
  }



  test("test4 - When partial cluster is not available/corrupted/deleted") {
    // check for row and column type

    // 1. what if one of diskstores is deleted - not available.
    // 2. what if some .crf files are missing
    // 3. what if some .drf files are missing
    // 4. what if some .krf files are missing

  }

*/

  test("test5 - Data export performance check")
  {

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
