/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.cluster

import java.io.{File, FileFilter}
import java.nio.file.{Files, Paths}
import java.sql.{Connection, SQLException, Statement}
import java.util.Properties

import scala.collection.mutable
import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.Constant
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion
import io.snappydata.test.dunit._
import io.snappydata.util.TestUtils
import org.apache.commons.io.FileUtils

import org.apache.spark.{SparkContext, SparkUtilsAccess}
import org.apache.spark.sql.types.{IntegerType, StructField}
import org.apache.spark.sql._

class SplitClusterDUnitSecurityTest(s: String)
    extends DistributedTestBase(s)
        with SplitClusterDUnitTestBase
        with Serializable
        with SnappyJobTestSupport {

  private[this] var ldapProperties: Properties = new Properties()
  private var restartLdap = false

  private val embeddedColTab1 = "EMBEDDEDCOLTAB1"
  private val smartColTab1 = "SMARTCOLTAB1"
  private val embeddedRowTab1 = "EMBEDDEDROWTAB1"
  private val smartRowTab1 = "SMARTROWTAB1"

  private[this] val bootProps: Properties = new Properties()
  bootProps.setProperty("log-file", "snappyStore.log")
  bootProps.setProperty("log-level", "info")
  bootProps.setProperty("statistic-archive-file", "snappyStore.gfs")
  bootProps.setProperty("spark.executor.cores", TestUtils.defaultCores.toString)
  System.setProperty(Constant.COMPRESSION_MIN_SIZE, compressionMinSize)

  var adminConn = null: Connection
  var user1Conn = null: Connection
  var user2Conn = null: Connection
  var user4Conn = null: Connection
  var snc = null: SnappyContext

  private[this] var host: Host = _
  var vm0: VM = _
  var vm1: VM = _
  var vm2: VM = _
  var vm3: VM = _

  val jdbcUser1 = "gemfire1"
  val jdbcUser2 = "gemfire2"
  val jdbcUser3 = "gemfire3"
  val jdbcUser4 = "gemfire4"
  val jdbcUser5 = "gemfire5"
  val jdbcUser6 = "gemfire6"
  val jdbcUser7 = "gemfire7"
  val jdbcUser8 = "gemfire8"
  val jdbcUser9 = "gemfire9"
  val adminUser1 = "gemfire10"

  val group1 = "gemGroup1"
  val group2 = "gemGroup2"

  // Job config names
  val outputFile = "output.file"
  val opCode = "op.code"
  val otherColTabName = "other.columntable"
  val otherRowTabName = "other.rowtable"

  override def setUp(): Unit = {
    super.setUp()
  }

  override def tearDown2(): Unit = {
    super.tearDown2()
    if (adminConn != null) adminConn.close()
    if (user1Conn != null) user1Conn.close()
    if (user2Conn != null) user2Conn.close()
    if (user4Conn != null) user4Conn.close()
    if (snc != null) snc.sparkContext.stop()
    if (restartLdap) {
      restartLdap = false
      stopLdapTestServer()
      setSecurityProps()
    }
  }

  def setSecurityProps(): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0,
      adminUser1, getClass.getResource("/auth.ldif").getPath)
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
  }

  def startArgs: Array[AnyRef] = Array(
    SplitClusterDUnitSecurityTest.locatorPort, bootProps).asInstanceOf[Array[AnyRef]]

  override val snappyProductDir = testObject.getEnvironmentVariable("SNAPPY_HOME")

  override val jobConfigFile = s"$snappyProductDir/conf/job.config"

  override protected val sparkProductDir: String =
    testObject.getEnvironmentVariable("APACHE_SPARK_HOME")

  protected val currentProductDir: String =
    testObject.getEnvironmentVariable("APACHE_SPARK_CURRENT_HOME")

  override def locatorClientPort: Int = { SplitClusterDUnitSecurityTest.locatorNetPort }

  override def startNetworkServers(): Unit = {}

  override protected def testObject = SplitClusterDUnitSecurityTest

  override def beforeClass(): Unit = {
    super.beforeClass()

    setSecurityProps()
    SplitClusterDUnitSecurityTest.bootExistingAuthModule(ldapProperties)

    logInfo(s"Starting snappy cluster in $snappyProductDir/work")
    // create locators, leads and servers files
    val port = SplitClusterDUnitSecurityTest.locatorPort
    val netPort = SplitClusterDUnitSecurityTest.locatorNetPort
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val confDir = s"$snappyProductDir/conf"
    val compressionArg = this.compressionArg
    val waitForInit = "-jobserver.waitForInitialization=true"
    val ldapConf = getLdapConf
    val syspropExtTableAuthz = "-DCHECK_EXTERNAL_TABLE_AUTHZ=true"
    writeToFile(
      s"localhost  -peer-discovery-port=$port -client-port=$netPort $compressionArg $ldapConf",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $waitForInit $compressionArg $ldapConf" +
      s" $syspropExtTableAuthz", s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort1 $compressionArg $ldapConf
          |localhost  -locators=localhost[$port] -client-port=$netPort2 $compressionArg $ldapConf
          |""".stripMargin, s"$confDir/servers")
    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    SplitClusterDUnitSecurityTest.startSparkCluster(sparkProductDir)
  }

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

  override def afterClass(): Unit = {
    super.afterClass()
    SplitClusterDUnitSecurityTest.stopSparkCluster(sparkProductDir)

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)

    stopLdapTestServer()

    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "job.config"))
    FileUtils.moveDirectory(new File(s"$snappyProductDir/work"), new File
    (s"$snappyProductDir/work-snap-1957"))
  }

  def stopLdapTestServer(): Unit = {
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
  }

  override def testConcurrentOpsOnColumnTables(): Unit = {}

  override def testColumnTableCreation(): Unit = {}

  override def testRowTableCreation(): Unit = {}

  override def testTableFormChanges(): Unit = {}

  override def testComplexTypesForColumnTables_SNAP643(): Unit = {}

  override def testUpdateDeleteOnColumnTables(): Unit = {}

  // Test to make sure that stock spark-shell works with SnappyData core jar
  def testSparkShell(): Unit = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    SplitClusterDUnitTest.invokeSparkShell(snappyProductDir, sparkProductDir,
      locatorClientPort, props)
  }

  // Test to make sure that stock spark-shell for latest Spark release works with JDBC pool jar
  def testSparkShellCurrent(): Unit = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    SplitClusterDUnitTest.invokeSparkShellCurrent(snappyProductDir, sparkProductDir,
      currentProductDir, locatorClientPort, props, vm = null /* SparkContext in current VM */)
  }

  def testPreparedStatements(): Unit = {
    def executePrepStmt(sql: String): Unit = {
      val ps = user1Conn.prepareStatement(sql)
      ps.setInt(1, 1000)
      ps.execute
      val rs = ps.getResultSet
      if (rs != null) {
        while (rs.next()) {}
        rs.close()
      }
      ps.close()
    }

    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    user1Conn = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    var stmt = user1Conn.createStatement()

    SplitClusterDUnitTest.createTableUsingJDBC(embeddedColTab1, "column", user1Conn, stmt,
      Map("COLUMN_BATCH_SIZE" -> "1k"), false)
    SplitClusterDUnitTest.createTableUsingJDBC(embeddedRowTab1, "row", user1Conn, stmt,
      Map.empty, false)

    executePrepStmt(s"insert into $embeddedColTab1 values (?, 'ten thousand', 1000.23)")
    executePrepStmt(s"insert into $embeddedRowTab1 values (?, 'ten thousand', 1000.23)")
    user1Conn.close()
    user1Conn = SplitClusterDUnitTest.getConnection(locatorClientPort, props)

    executePrepStmt(s"select * from $embeddedColTab1 where col1 = ? limit 20")
    executePrepStmt(s"select * from $embeddedRowTab1 where col1 = ? limit 20")
    stmt = user1Conn.createStatement()
    stmt.execute(s"drop table $embeddedColTab1")
    stmt.execute(s"drop table $embeddedRowTab1")
  }

  /**
    * Create row and column tables in embedded mode. Perform select, insert, update, delete and
    * drop from smart side and vice versa.
    */
  def testSQLOpsWithValidCredentials(): Unit = {
    user1Conn = getConn(jdbcUser1, true)
    val stmt = user1Conn.createStatement()
    val value = "brought up to zero"

    try {
      // Create row and column tables in embedded mode
      SplitClusterDUnitTest.createTableUsingJDBC(embeddedColTab1, "column", user1Conn, stmt,
        Map("COLUMN_BATCH_SIZE" -> "1k"), false)
      SplitClusterDUnitTest.createTableUsingJDBC(embeddedRowTab1, "row", user1Conn, stmt,
        Map.empty, false)

      // insert
      snc.sql(s"insert into $embeddedColTab1 values (10000, 'ten thousand', 1000.23)")
      snc.sql(s"insert into $embeddedRowTab1 values (10000, 'ten thousand', 1000.23)")
      // select
      var count = snc.sql(s"select count(*) from $embeddedColTab1").collect()(0).getLong(0)
      assert(count == 1, s"expected 1 rows but found $count in $embeddedColTab1")
      count = snc.sql(s"select count(*) from $embeddedRowTab1").collect()(0).getLong(0)
      assert(count == 1, s"expected 1 rows but found $count in $embeddedRowTab1")

      // update
      snc.sql(s"update $embeddedColTab1 set col1 = 5000 where col1 = 10000")
      var col1 = snc.sql(s"select * from $embeddedColTab1").collect()(0).get(0)
      assert(col1 == 5000, s"Update failed in $embeddedColTab1, found value $col1")
      snc.sql(s"update $embeddedRowTab1 set col1 = 5000 where col1 = 10000")
      col1 = snc.sql(s"select * from $embeddedRowTab1").collect()(0).get(0)
      assert(col1 == 5000, s"Update failed in $embeddedRowTab1, found value $col1")

      // delete
      snc.sql(s"delete from $embeddedColTab1 where col1 = 5000").collect()
      var rows = snc.sql(s"select * from $embeddedColTab1").collect().length
      assert(rows == 0, s"expected 0 rows but found $rows in $embeddedColTab1")
      snc.sql(s"delete from $embeddedRowTab1 where col1 = 5000")
      rows = snc.sql(s"select * from $embeddedRowTab1").collect().length
      assert(rows == 0, s"expected 0 rows but found $rows in $embeddedRowTab1")

      // insert more data. Adds 1005 rows each
      SplitClusterDUnitTest.populateTable(embeddedColTab1, user1Conn)
      SplitClusterDUnitTest.populateTable(embeddedRowTab1, user1Conn)

      rows = snc.sql(s"select * from $embeddedColTab1").collect().length
      assert(rows == 1005, s"expected 1005 rows but found $rows in $embeddedColTab1")
      rows = snc.sql(s"select * from $embeddedRowTab1").collect().length
      assert(rows == 1005, s"expected 1005 rows but found $rows in $embeddedRowTab1")

      // drop
      snc.sql(s"drop table $embeddedColTab1")
      snc.sql(s"drop table $embeddedRowTab1")
      assert(!snc.sparkSession.catalog.tableExists(embeddedColTab1),
        s"$embeddedColTab1 not dropped")
      assert(!snc.sparkSession.catalog.tableExists(embeddedRowTab1),
        s"$embeddedRowTab1 not dropped")

      // vice versa: Create row and column table in smart connector mode
      snc.sql(s"create table $smartColTab1 (col1 INT, col2 STRING, col3 DECIMAL) using column")
      snc.sql(s"create table $smartRowTab1 (col1 INT, col2 STRING, col3 DECIMAL) using row")

      // insert. Adds 1005 rows each
      SplitClusterDUnitTest.populateTable(smartColTab1, user1Conn)
      SplitClusterDUnitTest.populateTable(smartRowTab1, user1Conn)

      // select
      def checkCount(sql: String, count: Int, eq: Boolean = true): Unit = {
        stmt.execute(sql)
        rows = 0
        val rs = stmt.getResultSet
        while (rs.next()) rows += 1
        if (eq) {
          assert(rows == count, s"expected $count rows but found $rows for $sql")
        } else {
          assert(rows != count, s"expected less than $count rows but found $rows for $sql")
        }
      }
      checkCount(s"select * from $smartColTab1", 1005)
      checkCount(s"select * from $smartRowTab1", 1005)

      // update
      def checkCol1(sql: String): Unit = {
        stmt.execute(sql)
        val rs = stmt.getResultSet
        rs.next()
        assert(rs.getInt(1) == 0, s"Update failed. Col1 value is ${rs.getInt(0)}, but expected 0 " +
            s"for $sql")
      }
      stmt.execute(s"update $smartColTab1 set col1 = 0, col2 = '$value' where col1 < 0")
      checkCol1(s"select * from $smartColTab1 where col2 = '$value'")
      stmt.execute(s"update $smartRowTab1 set col1 = 0, col2 = '$value' where col1 < 0")
      checkCol1(s"select * from $smartRowTab1 where col2 = '$value'")

      // delete
      stmt.execute(s"delete from $smartColTab1 where col1 = 0")
      checkCount(s"select * from $smartColTab1", 1005, false)
      stmt.execute(s"delete from $smartRowTab1 where col1 = 0")
      checkCount(s"select * from $smartRowTab1", 1005, false)

      // drop
      stmt.execute(s"drop table $smartColTab1")
      stmt.execute(s"drop table $smartRowTab1")
      assertTableDeleted(() => {
        snc.sparkSession.catalog.refreshTable(smartColTab1)
      }, smartColTab1)
      assertTableDeleted(() => {
        snc.sparkSession.catalog.refreshTable(smartRowTab1)
      }, smartRowTab1)
    } finally {
      snc.sparkContext.stop()
      stmt.close()
      user1Conn.close()
    }
  }

  private def assertTableDeleted(func: () => Unit, t: String): Unit = {
    try {
      func()
      assert(false, s"Failed to drop $t")
    } catch {
      case te: TableNotFoundException =>
    }
  }

  def getConn(u: String, setSNC: Boolean = false): Connection = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, u)
    props.setProperty(Attribute.PASSWORD_ATTR, u)
    if (setSNC) {
      if (snc != null) snc.sparkContext.stop()
      snc = testObject.getSnappyContextForConnector(locatorClientPort, props)
    }
    SplitClusterDUnitTest.getConnection(locatorClientPort, props)
  }

  def permit(session: SnappySession, permit: String, op: String, t1: String, t2: String,
      user: String): Unit = {
    val toFrom = if (permit.equalsIgnoreCase("grant")) "to" else "from"
    Seq(t1, t2).foreach(t => session.sql(s"$permit $op on table $t $toFrom $user"))
    if (op.equalsIgnoreCase("insert") || op.equalsIgnoreCase("update")
        || op.equalsIgnoreCase("delete")) {
      // We need select permission for insert, update and delete operation
      Seq(t1, t2).foreach(t => session.sql(s"$permit select on table $t $toFrom $user"))
      if (op.equalsIgnoreCase("update")) {
        Seq(t1, t2).foreach(t => session.sql(s"$permit insert on table $t $toFrom $user"))
      }
    }
  }

  def permitConn(stmt: Statement, permit: String, op: String, t1: String, t2: String,
      user: String): Unit = {
    val toFrom = if (permit.equalsIgnoreCase("grant")) "to" else "from"
    Seq(t1, t2).foreach(t => stmt.execute(s"$permit $op on table $t $toFrom $user"))
    if (op.equalsIgnoreCase("insert") || op.equalsIgnoreCase("update")
        || op.equalsIgnoreCase("delete")) {
      // We need select permission for insert, update and delete operation
      Seq(t1, t2).foreach(t => stmt.execute(s"$permit select on table $t $toFrom $user"))
      if (op.equalsIgnoreCase("update")) {
        Seq(t1, t2).foreach(t => stmt.execute(s"$permit insert on table $t $toFrom $user"))
      }
    }
  }

  def executeSQL(stmt: Statement, s: String): Unit = {
    logInfo("Executing SQL: " + s)
    stmt.execute(s)
    val rs = stmt.getResultSet
    if (rs ne null) {
      while (rs.next()) {}
      rs.close()
    }
  }

  private def doSimpleStuffOnExtTable(user: String,
      fqtn: String, session: SnappySession, expectException: Boolean = false): Unit = {
    val sql = s"select count(*) from $fqtn"

    // Verify in smart connector mode
    try {
      session.sql(sql)
      if (expectException) assert(false, "Expected an exception but none found!")
    } catch {
      case ae: AnalysisException => {
        if (expectException) {
          // SQLState should be
          assert(ae.getMessage.contains(s"$user  not authorized to access"),
            s"Actual message: ${ae.getMessage}")
        } else {
          assert(false, s"Did not expect exception on table $fqtn: ${ae.getMessage()}")
        }
      }
    } finally {
      session.clear()
      session.close()
    }

    // Verify in embedded mode
    val conn = getConn(user, false)
    try {
      conn.createStatement().execute(sql)
      assert (!expectException, "Expected an exception but none found!")
    } catch {
      case sqle: SQLException => {
        if (expectException) {
          // SQLState should be
          val names = fqtn.split("\\.")
          assert(sqle.getMessage.contains(s"User '$user' does not have external table " +
              s"permission on  '${names(0)}'.'${names(1)}'"), s"Actual message: ${sqle.getMessage}")
        } else {
          assert(false, s"Did not expect exception on table $fqtn: ${sqle.getMessage}")
        }
      }
    } finally {
      conn.close()
    }
  }

  def testExtTableGrantRevoke(): Unit = {
    System.setProperty("CHECK_EXTERNAL_TABLE_AUTHZ", "true")
    try {
      val t1 = "t1_ext"
      if (adminConn == null) adminConn = getConn(adminUser1, true)
      doTest(adminConn.createStatement(), t1, s"$adminUser1.$t1", adminUser1)
      val jdbc5Conn = getConn(jdbcUser5)
      try {
        doTest(jdbc5Conn.createStatement(), t1, s"$jdbcUser5.$t1", jdbcUser5)
      } finally {
        jdbc5Conn.close()
      }
    } finally {
      System.setProperty("CHECK_EXTERNAL_TABLE_AUTHZ", "false")
    }
  }

  private def doTest(st: Statement, t1: String, fqtn: String, schemaOwner: String): Unit = {
    st.execute(s"create external table $t1 using csv options(path " +
      s"'${getClass.getResource("/northwind/orders.csv").getPath}', header 'true', " +
      s"inferschema 'true', maxCharsPerColumn '4096')")
    val group3 = Seq(jdbcUser6, jdbcUser7, jdbcUser8)
    val group6 = Seq(jdbcUser1, jdbcUser2, jdbcUser3, jdbcUser6, jdbcUser9)

    def ss(u: String): SnappySession = {
      snc.snappySession.conf.set(Constant.SPARK_STORE_PREFIX + Attribute.USERNAME_ATTR, u)
      snc.snappySession.conf.set(Constant.SPARK_STORE_PREFIX + Attribute.PASSWORD_ATTR, u)
      snc.snappySession
    }

    doSimpleStuffOnExtTable(adminUser1, fqtn, ss(adminUser1))
    if (!schemaOwner.equals(adminUser1)) { // schema owner is different from admin
      doSimpleStuffOnExtTable(schemaOwner, fqtn, ss(schemaOwner))
    }
    doSimpleStuffOnExtTable(jdbcUser3, fqtn, ss(jdbcUser3), true)
    st.execute(s"grant all on $t1 to $jdbcUser3")
    doSimpleStuffOnExtTable(jdbcUser3, fqtn, ss(jdbcUser3))
    st.execute(s"revoke all on $t1 from $jdbcUser3")
    doSimpleStuffOnExtTable(jdbcUser3, fqtn, ss(jdbcUser3), true)

    // Grant all to gemGroup3 (gemfire6,7,8) and gemGroup6 (gemfire1,2,3,6,9)
    // Before grant, expect exception
    (group3 ++ group6).foreach(u => doSimpleStuffOnExtTable(u, fqtn, ss(u), true))

    st.execute(s"grant all on $fqtn to LDAPGROUP:gemGroup3,ldapgroup:gemGroup6")
    (group3 ++ group6).foreach(u => doSimpleStuffOnExtTable(u, fqtn, ss(u)))

    // Now revoke all from group3 and expect exception
    st.execute(s"revoke all on $t1 from $jdbcUser3,ldapgroup:gemGroup3")
    Seq(jdbcUser7, jdbcUser8).foreach(u => doSimpleStuffOnExtTable(u, fqtn,
      ss(u), true))
    // gemGroup6 still has grants
    group6.foreach(u => doSimpleStuffOnExtTable(u, fqtn, ss(u)))

    st.execute(s"revoke all on $t1 from ldapGroup:gemGroup6")
    group6.foreach(u => doSimpleStuffOnExtTable(u, fqtn, ss(u), true))

    // Only admin and schema owner should have permissions
    doSimpleStuffOnExtTable(adminUser1, fqtn, ss(adminUser1))
    if (!schemaOwner.equals(adminUser1)) {
      doSimpleStuffOnExtTable(schemaOwner, fqtn, ss(schemaOwner))

      // Verify permissions are retained even after the cluster is restarted.
      st.execute(s"grant all on $fqtn to LDAPGROUP:gemGroup3")
      restartCluster()
      getConn(jdbcUser1, true).close() // Just initialize snc.
      group3.foreach(u => doSimpleStuffOnExtTable(u, fqtn, ss(u)))
      doSimpleStuffOnExtTable(jdbcUser9, fqtn, ss(jdbcUser9), true)

      // Verify admin can grant permissions on external tables of other users.
      val aConn = getConn(adminUser1, false)
      try {
        val aSt = aConn.createStatement()
        aSt.execute(s"grant all on $fqtn to gemfire9")
        doSimpleStuffOnExtTable(jdbcUser9, fqtn, ss(jdbcUser9))
        aSt.execute(s"revoke all on $fqtn from gemfire9")
        doSimpleStuffOnExtTable(jdbcUser9, fqtn, ss(jdbcUser9), true)
      } finally {
        aConn.close()
      }
    } else {
      // This is admin connection. Test REFRESH_LDAP_GROUP works fine (SNAP-3281)
      st.execute("call SYS.REFRESH_LDAP_GROUP('gemGroup1')")
    }
  }

  /**
    * Grant and revoke select, insert, update and delete operations and verify from smart and
    * embedded side.
    *
    * Attempt to modify hive metastore via a thin connection should fail.
    */
  def testGrantRevokeAndHiveModification(): Unit = {
    user1Conn = getConn(jdbcUser1)
    val user1Stmt = user1Conn.createStatement()
    val value = "brought up to zero"

    user2Conn = getConn(jdbcUser2, setSNC = true)
    var user2Stmt = user2Conn.createStatement()

    adminConn = getConn(adminUser1)
    val adminStmt = adminConn.createStatement()

    SplitClusterDUnitTest.createTableUsingJDBC(embeddedColTab1, "column", user1Conn, user1Stmt,
      Map("COLUMN_BATCH_SIZE" -> "1k"))
    SplitClusterDUnitTest.createTableUsingJDBC(embeddedRowTab1, "row", user1Conn, user1Stmt)

    // All DMLs from another user should fail
    def assertFailure(sql: () => Unit, s: String): Unit = {
      val states = Seq("42502", "42500")
      assertFailures(sql, s, states)
    }

    val sqls = List(s"select * from $jdbcUser1.$embeddedColTab1",
      s"select * from $jdbcUser1.$embeddedRowTab1",
      s"select count(*) from $jdbcUser1.$embeddedColTab1",
      s"insert into $jdbcUser1.$embeddedColTab1 values (1, '$jdbcUser2', 1.1)",
      s"insert into $jdbcUser1.$embeddedRowTab1 values (1, '$jdbcUser2', 1.1)",
      s"update $jdbcUser1.$embeddedColTab1 set col1 = 0, col2 = '$value by $jdbcUser2' where " +
          s"col3 < 1.0",
      s"update $jdbcUser1.$embeddedRowTab1 set col1 = 0, col2 = '$value by $jdbcUser2' where " +
          s"col3 < 1.0",
      s"delete from $jdbcUser1.$embeddedColTab1 where col1 = 0",
      s"delete from $jdbcUser1.$embeddedRowTab1 where col1 = 0"
    )

    sqls.foreach(s => assertFailure(() => {executeSQL(user2Stmt, s)}, s))
    val snap1849 = s"select count(*) from $jdbcUser1.$embeddedRowTab1";
    // Verify that it fails in embedded case
    assertFailure(() => {executeSQL(user2Stmt, snap1849)}, snap1849)
    // Verify that it fails in smart connector case - TODO Not yet fixed
    // assertFailure(() => {snc.sql(s).collect()}, snap1849)
    sqls.foreach(s => assertFailure(() => {snc.sql(s).collect()}, s))

    def verifyGrantRevoke(op: String, sqls: List[String]): Unit = {
      // grant
      permitConn(user1Stmt, "grant", op, embeddedColTab1, embeddedRowTab1, jdbcUser2)
      sqls.foreach(s => executeSQL(user2Stmt, s))
      sqls.foreach(s => snc.sql(s).collect())

      // revoke
      permitConn(user1Stmt, "revoke", op, embeddedColTab1, embeddedRowTab1, jdbcUser2)
      sqls.foreach(s => assertFailure(() => {executeSQL(user2Stmt, s)}, s))
      sqls.foreach(s => assertFailure(() => {snc.sql(s).collect()}, s))
      sqls.foreach(s => executeSQL(adminStmt, s))
    }

    verifyGrantRevoke("select", List(sqls(0), sqls(1), sqls(2)))
    verifyGrantRevoke("insert", List(sqls(3), sqls(4)))
    // No update on column tables
    verifyGrantRevoke("update", List(sqls(5), sqls(6)))
    // No delete on column tables
    verifyGrantRevoke("delete", List(sqls(7), sqls(8)))

    // SNAPPY_HIVE_METASTORE should not be modifiable by users.
    val sql = s"insert into ${Misc.SNAPPY_HIVE_METASTORE}.VERSION values (1212, 'NA', 'NA')"
    assertFailure(() => {user1Stmt.execute(sql)}, sql)

    // SNAP-1876 Verify grant/revoke are retained after cluster restart
    executeSQL(user1Stmt, s"grant select on table $embeddedColTab1 to $jdbcUser2")
    executeSQL(user1Stmt, s"revoke select on table $embeddedColTab1 from $jdbcUser2")
    executeSQL(user1Stmt, s"grant select on table $embeddedRowTab1 to $jdbcUser2")
    executeSQL(user1Stmt, s"grant insert on table $embeddedRowTab1 to $jdbcUser2")

    restartCluster()
    user2Conn = getConn(jdbcUser2, true)
    user2Stmt = user2Conn.createStatement()
    assertFailure(() => {executeSQL(user2Stmt, sqls(0))}, sqls(0)) // select on embeddedColTab1
    assertFailure(() => {snc.sql(sqls(0)).collect()}, sqls(0)) // select on embeddedColTab1
    executeSQL(user2Stmt, sqls(4)) // insert into embeddedRowTab1
    snc.sql(sqls(4)).collect() // insert into embeddedRowTab1
    assertFailure(() => {executeSQL(user2Stmt, sqls(8))}, sqls(8)) // delete on embeddedRowTab1
    assertFailure(() => {snc.sql(sqls(7)).collect()}, sqls(7)) // delete on embeddedColTab1
  }

  def restartCluster(): Unit = {
    if (user1Conn != null) user1Conn.close()
    if (user2Conn != null) user2Conn.close()
    if (user4Conn != null) user4Conn.close()
    adminConn.close()
    adminConn = null
    snc.sparkContext.stop()
    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)
    var waitSeconds = 30
    var status = "stopped"

    val wc = new WaitCriterion {
      override def done() = {
        val output = (snappyProductDir + "/sbin/snappy-status-all.sh").!!
        logInfo(s"Status output: \n$output")
        getCount(output, status) == 4
      }
      override def description(): String = s"All nodes not in $status state after " +
        s"$waitSeconds seconds."
    }
    DistributedTestBase.waitForCriterion(wc, waitSeconds * 1000, 1000, true)

    logInfo(s"Starting snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)
    waitSeconds = 60
    status = "running"
    DistributedTestBase.waitForCriterion(wc, waitSeconds * 1000, 1000, true)
  }

  def getCount(source: String, token: String): Int = {
    if (source.contains(token)) {
      1 + getCount(source.substring(source.indexOf(token) + 1), token)
    } else {
      0
    }
  }

  /**
    * Attempt to get a snappysession or connection with invalid credentials should fail.
    */
  def _testWithInvalidCredentials(): Unit = {
    val props = new Properties()

    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)

    def attemptConn(p: String, s: String): Unit = {
      props.setProperty(p, s)
      try {
        SplitClusterDUnitTest.getConnection(locatorClientPort, props)
        assert(false, s"JDBC conn should have failed with $p: $s")
      } catch {
        case e: Throwable => logInfo(s"Expected exception while getting a JDBC connection $e")
      }
      try {
        testObject.getSnappyContextForConnector(locatorClientPort, props)
        assert(false, s"Smart conn should have failed with $p: $s")
      } catch {
        case e: Throwable => logInfo(s"Expected exception while getting a smart connection $e")
      }
    }

    List("invalid_password", "").foreach(e => attemptConn(Attribute.PASSWORD_ATTR, e))
    props.setProperty(Attribute.PASSWORD_ATTR, adminUser1)
    List("invalid_user", "").foreach(e => attemptConn(Attribute.USERNAME_ATTR, e))
    restartLdap = true;
  }

  /**
    * Use APIs to perform DDLs and DMLs from smart side.
    *
    * DDLs: create, drop, alter tables
    *
    * DMLs: select, insert, update, delete rows
    */
  def testAPIsWithValidCredentials(): Unit = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)

    snc = testObject.getSnappyContextForConnector(locatorClientPort, props)
    val sns = snc.snappySession

    val rdd = sns.sparkContext.parallelize(
      (1 to 113999).map(i => Data2(i, s"my_name_$i", s"my_work_address_$i")))
    val dataDF = sns.createDataFrame(rdd)
    val col1 = "COL1"
    val col2 = "COL2"
    val col3 = "COL3"
    val col4 = "COL4"

    sns.createTable(smartColTab1, "column", dataDF.schema, Map("COLUMN_BATCH_SIZE" -> "10k"))
    sns.createTable(smartRowTab1, "row", dataDF.schema, Map("PARTITION_BY" -> "COL1"))
    sns.catalog.refreshTable(smartColTab1)
    sns.catalog.refreshTable(smartRowTab1)

    sns.insert(smartRowTab1, Row(1, "one", "Don Bosco Road"))
    sns.insert(smartColTab1, Row(1, "one", "Don Bosco Road"))
    sns.insert(smartColTab1, Row(1000, "Something to make it more than the column batch size " +
        "which is set to be fifty bytes above", "Some address"))
    var rows = sns.sql(s"select * from $smartColTab1").collect().length
    assert(rows == 2, s"expected 2 rows after insert, found $rows")

    assert(sns.put(smartRowTab1, Row(1, "Don Bosco", "Off Airport Road")) == 1, "Put failed")
    assert(sns.put(smartRowTab1, Row(1000, "Don Bosco Jr.", "Off Airport Road")) == 1, "Put " +
        "failed")

    val nameExpected = "Updated this row which had id = 1000"
    assert(sns.update(smartRowTab1, s"$col1 = 1000", Row(nameExpected), col2) == 1,
      "Update failed")

    assert(sns.delete(smartRowTab1, s"$col1 = 1000") == 1, "Delete failed")

    dataDF.write.insertInto(smartColTab1)
    dataDF.write.insertInto(smartRowTab1)

    sns.sqlContext.alterTable(smartRowTab1, true, StructField(col4, IntegerType, true))

    sns.dropTable(smartColTab1, false)
    assertTableDeleted(() => {sns.catalog.refreshTable(smartColTab1)}, smartColTab1)
    sns.dropTable(smartRowTab1, false)
    assertTableDeleted(() => {sns.catalog.refreshTable(smartRowTab1)}, smartRowTab1)
  }

  /**
   * Create a schema owned by group1, create table and index and execute DMLs on it by a user
   * of that group.
   * Repeat that by another member of the same group and ensure it succeeds.
   * Repeat that by a member of a different group and verify it fails.
   * Grant DML permissions on some tables to another group and ensure it works.
   * Revoke those permissions on tables from that group and ensure it works too.
   */
  def testLDAPGroupOwnershipJDBC(): Unit = {
    adminConn = getConn(adminUser1)
    user1Conn = getConn(jdbcUser1)
    user2Conn = getConn(jdbcUser2)
    user4Conn = getConn(jdbcUser4)

    var stmt = adminConn.createStatement()
    var user1Stmt = user1Conn.createStatement()
    var user2Stmt = user2Conn.createStatement()
    var user4Stmt = user4Conn.createStatement()

    val schema = "groupSchema"
    val t1 = "gemone" // column table to be created by gemfire1
    val t1r = "gemonerow" // row table to be created by gemfire1
    val t2 = "gemtwo" // column table to be created by gemfire2
    val t2r = "gemtworow" // row table to be created by gemfire2

    // admin user
    executeSQL(stmt, s"create schema $schema authorization ldapgroup:$group1")

    // user gemfire1 of group gemGroup1
    Seq(s"create table $schema.$t1 (id int, name string) using column",
      s"select * from $schema.$t1",
      s"insert into $schema.$t1 values (1, 'one'), (2, 'two'), (3, 'three')," +
          s" (4, 'four'), (5, 'five')",
      s"update $schema.$t1 set id = 10 where name like 'one'",
      s"select * from $schema.$t1",
      s"delete from $schema.$t1 where id = 10",
      s"select * from $schema.$t1",
      s"create table $schema.$t1r (id int, name string)",
      s"CREATE VIEW $schema.${t1}view AS SELECT id, name FROM $schema.$t1",
      s"CREATE TEMPORARY VIEW ${t1}viewtemp AS SELECT id, name FROM $schema.$t1",
      s"CREATE GLOBAL TEMPORARY VIEW ${t1}viewtempg AS SELECT id, name FROM $schema.$t1",
      s"CREATE TEMPORARY TABLE ${t1}temp AS SELECT id, name FROM $schema.$t1",
      s"CREATE GLOBAL TEMPORARY TABLE ${t1}tempg AS SELECT id, name FROM $schema.$t1",
      s"CREATE EXTERNAL TABLE $schema.${t1}ext USING csv OPTIONS(path " +
          s"'../../quickstart/src/main/resources/customer.csv')")
        .foreach(executeSQL(user1Stmt, _))

    // check that triggers are neither allowed to be attached to column tables
    // or have column tables as target
    try {
      executeSQL(stmt, s"CREATE TRIGGER trig AFTER DELETE ON $schema.$t1 REFERENCING " +
          s"OLD AS OLD FOR EACH ROW DELETE FROM $schema.$t1 WHERE id = OLD.id")
      assert(assertion = false, "expected exception creating trigger on column table")
    } catch {
      case sqle: SQLException if sqle.getSQLState == "0A000" => // expected
    }
    try {
      executeSQL(stmt, s"CREATE TRIGGER trig AFTER DELETE ON $schema.$t1r REFERENCING " +
          s"OLD AS OLD FOR EACH ROW DELETE FROM $schema.$t1 WHERE id = OLD.id")
      assert(assertion = false, "expected exception with trigger targeting column table")
    } catch {
      case sqle: SQLException if sqle.getSQLState == "0A000" => // expected
    }

    // user gemfire2 of same group gemGroup1
    Seq(s"select * from $schema.$t1",
      s"create table $schema.$t2 (id int, name string) using column",
      s"create table $schema.$t2r (id int, name string)",
      s"show tables in $schema",
      s"select * from $schema.$t2",
      s"CREATE TRIGGER trig AFTER DELETE ON $schema.$t1r REFERENCING " +
          s"OLD AS OLD FOR EACH ROW DELETE FROM $schema.$t2r WHERE id = OLD.id",
      s"insert into $schema.$t2 values (1, '1'), (2, '2'), (3, '3')," +
          s" (4, '4'), (5, '5'), (6, '6')",
      s"select * from $schema.$t2",
      s"delete from $schema.$t1 where name like 'two'",
      s"drop table $schema.$t1r",
      s"create table $schema.$t1r (id int, name string)",
      s"select * from $schema.$t2").foreach(executeSQL(user2Stmt, _))

    // user gemfire1
    Seq(s"alter table $schema.$t2r drop column name",
      s"alter table $schema.$t2r add column personality varchar(10)")
        .foreach(executeSQL(user1Stmt, _))

    // user gemfire4 of different group
    executeSQL(user4Stmt, s"show tables in $schema")
    Seq(s"select * from $schema.$t1",
      s"create table $schema.gemfour (id int, name string) using column",
      s"CREATE TRIGGER trigfour AFTER DELETE ON $schema.$t1r REFERENCING " +
          s"OLD AS OLD FOR EACH ROW DELETE FROM $schema.$t2r WHERE id = OLD.id",
      s"insert into $schema.$t2 values (1, '1'), (2, '2'), (3, '3')," +
          s" (4, '4'), (5, '5'), (6, '6')",
      s"update $schema.$t1 set id = 100 where name like 'four'",
      s"delete from $schema.$t1 where name like 'two'",
      s"alter table $schema.$t2r add column address string",
      // Create view succeeds but select on it fails, which is comforting.
//      s"CREATE VIEW $schema.${t1}view4 AS SELECT id, name FROM $schema.$t1",
      s"CREATE EXTERNAL TABLE $schema.${t1}ext4 USING csv OPTIONS(path " +
          s"'../../quickstart/src/main/resources/customer.csv')",
      s"CREATE INDEX $schema.idx4 ON $schema.$t1 (id, name)")
        .foreach(sql => assertFailures(() => {
          executeSQL(user4Stmt, sql)
        }, sql, Seq("42500", "42502", "42506", "42507", "38000")))

    // Grant DML permissions to gemfire4 and ensure it works.
    executeSQL(user1Stmt, s"grant select on $schema.$t1 to ldapgroup:$group2")
    executeSQL(user1Stmt, s"grant select on $schema.$t2r to ldapgroup:$group2") // due to trigger
    executeSQL(user4Stmt, s"select * from $schema.$t1")
    executeSQL(user1Stmt, s"grant insert on $schema.$t1 to ldapgroup:$group2")
    executeSQL(user4Stmt, s"insert into $schema.$t1 values (111, 'gemfire4 111')," +
        s" (222, 'gemfire4 222')")
    executeSQL(user2Stmt, s"grant update on $schema.$t1 to ldapgroup:$group2")
    executeSQL(user4Stmt, s"update $schema.$t1 set name = 'gemfire4 111 updated' where id = 111")
    executeSQL(user2Stmt, s"grant delete on $schema.$t1 to ldapgroup:$group2")
    executeSQL(user2Stmt, s"grant delete on $schema.$t2r to ldapgroup:$group2") // due to trigger
    executeSQL(user4Stmt, s"delete from $schema.$t1 where id = 111")
    executeSQL(user2Stmt, s"grant trigger on $schema.$t1r to ldapgroup:$group2")
    executeSQL(user2Stmt, s"grant trigger on $schema.$t2r to ldapgroup:$group2")
    executeSQL(user4Stmt, s"CREATE TRIGGER trigfour AFTER DELETE ON $schema.$t1r REFERENCING " +
        s"OLD AS OLD FOR EACH ROW DELETE FROM $schema.$t2r WHERE id = OLD.id")

    // Revoke all and ensure it works too.
    Seq(s"revoke select on $schema.$t1 from ldapgroup:$group2",
      s"revoke insert on $schema.$t1 from ldapgroup:$group2",
      s"revoke update on $schema.$t1 from ldapgroup:$group2",
      s"revoke delete on $schema.$t1 from ldapgroup:$group2",
      s"revoke delete on $schema.$t2 from ldapgroup:$group2",
      s"revoke trigger on $schema.$t1r from ldapgroup:$group2",
      s"revoke trigger on $schema.$t2r from ldapgroup:$group2")
        .foreach(executeSQL(user1Stmt, _))
    Seq(s"select * from $schema.$t1",
      s"insert into $schema.$t1 values (111, 'gemfire4 111')," +
          s" (222, 'gemfire4 222')",
      s"update $schema.$t1 set name = 'gemfire4 111 updated' where id = 111",
      s"delete from $schema.$t1 where id = 111",
      s"CREATE TRIGGER trigfournew AFTER DELETE ON $schema.$t1r REFERENCING " +
          s"OLD AS OLD FOR EACH ROW DELETE FROM $schema.$t2r WHERE id = OLD.id")
        .foreach(sql => assertFailures(() => {
          executeSQL(user4Stmt, sql)
        }, sql, Seq("42500", "42502", "42506", "42507")))
  }

  def assertFailures(f: () => Unit, s: String, states: Seq[String]): Unit = {
    try {
      f()
      assert(false, s"Should have failed: $s")
    } catch {
      case sqle: SQLException =>
        if (states.contains(sqle.getSQLState)) {
          logInfo(s"Found expected error: $sqle")
        } else {
          logError(s"Found different SQLState: ${sqle.getSQLState}")
          throw sqle
        }
      case t: Throwable =>
        var okay = false
        states.foreach(state => {
          if (t.getMessage.contains(state)) {
            logInfo(s"Found expected error in: ${t.getClass.getName}, ${t.getMessage}")
            okay = true
          }
        })
        if (!okay) {
          logInfo(s"Found unexpected error in: ${t.getClass.getName}, ${t.getMessage}")
          throw t
        }
    }
  }

  def testMetastoreAccessAdminOnlySmartConn: Unit = {
    getConn(adminUser1, true)
    import org.scalatest.Assertions.intercept
    var thrown = intercept[ParseException] {
      snc.sql("select * from SNAPPY_HIVE_METASTORE.version")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))

    thrown = intercept[ParseException] {
      snc.sql("update SNAPPY_HIVE_METASTORE.version set version_comment = 'comment changed'")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))
    thrown = intercept[ParseException] {
      snc.sql("insert into SNAPPY_HIVE_METASTORE.version values (4, '1.2.3', 'dummy comment')")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))
    thrown = intercept[ParseException] {
      snc.sql("delete from SNAPPY_HIVE_METASTORE.version where ver_id = 2")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))


    getConn(jdbcUser1, true)
    thrown = intercept[ParseException] {
      snc.sql("select * from SNAPPY_HIVE_METASTORE.version")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))

    thrown = intercept[ParseException] {
      snc.sql("update SNAPPY_HIVE_METASTORE.version set version_comment = 'comment changed'")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))

    thrown = intercept[ParseException] {
      snc.sql("insert into SNAPPY_HIVE_METASTORE.version values (4, '1.2.3', 'dummy comment')")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))
    thrown = intercept[ParseException] {
      snc.sql("delete from SNAPPY_HIVE_METASTORE.version where ver_id = 2")
    }
    assert(thrown.getMessage().startsWith("Invalid input \"SNAPPY_HIVE_METASTORE.v\""))
  }

  def _testLDAPGroupOwnershipSmartConnector(): Unit = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, adminUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, adminUser1)

    snc = testObject.getSnappyContextForConnector(locatorClientPort, props)
    val sns = snc.snappySession

    sns.sql("create schema groupSchema authorization gemGroup1")
  }

  def testSnappyJob(): Unit = {
    // Create config file with credentials
    writeToFile(s"-u $jdbcUser1:$jdbcUser1", jobConfigFile)

    val className = "io.snappydata.cluster.jobs.SnappySecureJob"
    submitAndWaitForCompletion(className,
      s" --conf $opCode=sqlOps --conf $outputFile=SnappyValidJob.out")

    val colTab = "JOB_COLTAB"
    val rowTab = "JOB_ROWTAB"
    def submitJobWithOperation(op: String): Unit = {
      submitAndWaitForCompletion(className, s""" --conf $opCode=$op
       --conf $otherColTabName=$jdbcUser2.$colTab
        --conf $otherRowTabName=$jdbcUser2.$rowTab
        --conf $outputFile=Snappy${op}Job.out""")
    }

    user2Conn = getConn(jdbcUser2, setSNC = true)
    val stmt = user2Conn.createStatement()
    SplitClusterDUnitTest.createTableUsingJDBC(colTab, "column", user2Conn, stmt,
      Map("COLUMN_BATCH_SIZE" -> "1k"))
    SplitClusterDUnitTest.createTableUsingJDBC(rowTab, "row", user2Conn, stmt)

    submitJobWithOperation("nogrant") // tells job to verify DMLs without any explicit grant

    Seq("select", "insert", "update", "delete").foreach(dml => {
      permitConn(stmt, "grant", dml, colTab, rowTab, jdbcUser1)
      submitJobWithOperation(dml) // tells job to verify respective dml
      permitConn(stmt, "revoke", dml, colTab, rowTab, jdbcUser1)
    })

    Seq("select", "insert", "update", "delete").foreach(dml => {
      permit(snc.snappySession, "grant", dml, colTab, rowTab, jdbcUser1)
      submitJobWithOperation(dml) // tells job to verify respective dml
      permit(snc.snappySession, "revoke", dml, colTab, rowTab, jdbcUser1)
    })

    // Submit the same job with invalid credentials
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "job.config"))
    writeToFile(s"-u $jdbcUser1:invalid", jobConfigFile)
    logInfo(s"Re-submitting job $className with invalid credentials.")
    val consoleLog = submitJob(className, s" --conf $outputFile=SnappyInvalidJob.out")
    logInfo(consoleLog)
    assert(consoleLog.contains("The supplied authentication is invalid"), "Job should have failed")
  }

  def testSnappyStreamingJob(): Unit = {
    // Create config file with credentials
    writeToFile(s"-u $jdbcUser1:$jdbcUser1", jobConfigFile)
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.SnappyStreamingSecureJob",
      s" --stream --conf $opCode=sqlOps" +
          s" --conf $outputFile=SnappyStreamingValidJob.out")
  }

  def testSnappyJavaJob(): Unit = {
    // Create config file with credentials
    writeToFile(s"-u $jdbcUser1:$jdbcUser1", jobConfigFile)
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.SnappyJavaSecureJob",
      s" --conf $opCode=sqlOps" +
          s" --conf $outputFile=SnappyJavaValidJob.out")
  }

  def testSnappyJavaStreamingJob(): Unit = {
    // Create config file with credentials
    writeToFile(s"-u $jdbcUser1:$jdbcUser1", jobConfigFile)
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.SnappyJavaStreamingSecureJob",
      s" --stream --conf $opCode=sqlOps" +
          s" --conf $outputFile=SnappyJavaStreamingValidJob.out")
  }

  def _testUDFAndProcs(): Unit = {
  }

  def _testConcurrentUsers(): Unit = {
  }

  def testUDFSNAP_2636(): Unit = {
    // Build a jar with UDF
    val udf1Source = "public class StringLengthUDF1 implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return (s.length() - 1000); " +
        "} }"
    val file1 = SparkUtilsAccess.createUDFClass("StringLengthUDF1", udf1Source)
    val jar1 = SparkUtilsAccess.createJarFile(Seq(file1), "user1udf.jar", true)

    val udf2Source = "public class StringLengthUDF2 implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return (s.length() + 1000); " +
        "} }"
    val file2 = SparkUtilsAccess.createUDFClass("StringLengthUDF2", udf2Source)
    val jar2 = SparkUtilsAccess.createJarFile(Seq(file2))

    // Create function 1 with the jar with one user
    user1Conn = getConn(jdbcUser1)
    var stmt1 = user1Conn.createStatement()
    executeSQL(stmt1, s"CREATE FUNCTION myUDF AS " +
        s"StringLengthUDF1 returns Integer using jar '$jar1'")
    // Select with that UDF
    var stmt2 = user1Conn.createStatement()
    stmt2.execute(s"select myUDF('abcd')")
    var rs = stmt2.getResultSet
    if (rs ne null) {
      while (rs.next()) {
        assert(rs.getInt(1) == -996, s"Expected -996, found ${rs.getInt(1)}")
      }
      rs.close()
    }

    // Create function 2 with the jar with another user
    user2Conn = getConn(jdbcUser2)
    stmt1 = user2Conn.createStatement()
    executeSQL(stmt1, s"CREATE FUNCTION myUDF AS " +
        s"StringLengthUDF2 returns Integer using jar '$jar2'")
    // Select with that UDF
    stmt2 = user2Conn.createStatement()
    stmt2.execute(s"select myUDF('abcd')")
    rs = stmt2.getResultSet
    if (rs ne null) {
      while (rs.next()) {
        assert(rs.getInt(1) == 1004, s"Expected 1004, found ${rs.getInt(1)}")
      }
      rs.close()
    }

    // SNAP-3069
    stmt2 = user1Conn.createStatement()
    stmt2.execute(s"select myUDF('abcd')")
    rs = stmt2.getResultSet
    if (rs ne null) {
      while (rs.next()) {
        assert(rs.getInt(1) == -996, s"Expected -996, found ${rs.getInt(1)}")
      }
      rs.close()
    }

    // Verify list jars
    stmt2 = user2Conn.createStatement()
    stmt2.execute(s"list jars")
    rs = stmt2.getResultSet
    var rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        val udfName = rs.getString(1)
        assert(udfName.equalsIgnoreCase(s"[UDF]$jdbcUser1.myUDF")
            || udfName.equalsIgnoreCase(s"[UDF]$jdbcUser2.myUDF"),
          s"Unexpected UDF name $udfName")
      }
    }
    assert(rows == 2, s"Expected 2 UDFs, but found $rows")

    // Verify jar file paths
    val leadDir = new File(s"$snappyProductDir/work/localhost-lead-1/snappy-jars")
    val server1Dir = new File(s"$snappyProductDir/work/localhost-server-1")
    val server2Dir = new File(s"$snappyProductDir/work/localhost-server-2")

    leadDir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("myudf") && pathname.getName.contains("jar")
      }
    }).foreach(x => println(s"BEFORE DROP  [snappy-jars]: ${x.getAbsolutePath}"))
    server1Dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("myudf") && pathname.getName.contains("jar")
      }
    }).foreach(x => println(s"BEFORE DROP  [snappy-jars]: ${x.getAbsolutePath}"))
    server2Dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("myudf") && pathname.getName.contains("jar")
      }
    }).foreach(x => println(s"BEFORE DROP  [snappy-jars]: ${x.getAbsolutePath}"))


    // Drop a function of jdbcUser2
    executeSQL(stmt2, s"drop function myUDF")

    // Verify function does not execute
    stmt2 = user2Conn.createStatement()
    try {
      stmt2.execute(s"select myUDF('abcd')")
      assert(false, s"Expected an exception!")
    } catch {
      case _: SQLException => // expected
      case t: Throwable => assert(false, s"Unexpected exception $t")
    } finally {
      if (stmt2.getResultSet ne null) stmt2.getResultSet.close()
    }
    // Verify jar file paths
    leadDir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("myudf") && pathname.getName.contains("jar")
      }
    }).foreach(x => println(s"AFTER DROP  [snappy-jars]: ${x.getAbsolutePath}"))
    server1Dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("myudf") && pathname.getName.contains("jar")
      }
    }).foreach(x => println(s"AFTER DROP  [snappy-jars]: ${x.getAbsolutePath}"))
    server2Dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("myudf") && pathname.getName.contains("jar")
      }
    }).foreach(x => println(s"AFTER DROP  [snappy-jars]: ${x.getAbsolutePath}"))

    // Verify list jars
    stmt2.execute(s"list jars")
    rs = stmt2.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        val udfName = rs.getString(1)
        assert(udfName.equalsIgnoreCase(s"[UDF]$jdbcUser1.myUDF"),
          s"Unexpected UDF name $udfName")
      }
    }
    assert(rows == 1, s"Expected just 1 UDF, but found $rows")

    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)

    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    user1Conn = getConn(jdbcUser1)
    // Select with the existing UDF
    stmt2 = user1Conn.createStatement()
    stmt2.execute(s"select myUDF('abcd')")
    rs = stmt2.getResultSet
    if (rs ne null) {
      while (rs.next()) {
        assert(rs.getInt(1) == -996, s"Expected -996 post reboot, found ${rs.getInt(1)}")
      }
      rs.close()
    }

    user2Conn = getConn(jdbcUser2)
    // Select with the dropped UDF
    stmt2 = user2Conn.createStatement()
    try {
      stmt2.execute(s"select myUDF('abcd')")
      assert(false, s"Expected an exception!")
    } catch {
      case _: SQLException => // expected
      case t: Throwable => assert(false, s"Unexpected exception $t")
    } finally {
      if (stmt2.getResultSet ne null) stmt2.getResultSet.close()
    }

    // Verify list jars
    stmt2.execute(s"list jars")
    rs = stmt2.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        val udfName = rs.getString(1)
        assert(udfName.equalsIgnoreCase(s"[UDF]$jdbcUser1.myUDF"),
          s"Unexpected UDF name $udfName")
      }
    }

    val udf1SourceNew = "public class StringLengthUDF1 implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return (s.length() * 1000); " +
        "} }"
    val file1New = SparkUtilsAccess.createUDFClass("StringLengthUDF1", udf1SourceNew)
    val jar1New = SparkUtilsAccess.createJarFile(Seq(file1New), "user1udf.jar", true)

    stmt1 = user1Conn.createStatement()
    executeSQL(stmt1, s"DROP FUNCTION myUDF")

    stmt2 = user1Conn.createStatement()
    try {
      stmt2.execute(s"select myUDF('abcd')")
      assert(false, s"Expected an exception!")
    } catch {
      case _: SQLException => // expected
      case t: Throwable => assert(false, s"Unexpected exception $t")
    }

    // Verify list jars
    stmt2.execute(s"list jars")
    rs = stmt2.getResultSet
    if (rs ne null) {
      assert(!rs.next(), "'list jars' should output zero jars but didn't!")
    }

    executeSQL(stmt1, s"CREATE FUNCTION myUDF AS " +
        s"StringLengthUDF1 returns Integer using jar '$jar1New'")
    // Select with that UDF
    stmt2 = user1Conn.createStatement()
    stmt2.execute(s"select myUDF('11223344')")
    rs = stmt2.getResultSet
    if (rs ne null) {
      while (rs.next()) {
        assert(rs.getInt(1) == 8000, s"Expected 8000 with new source, found ${rs.getInt(1)}")
      }
      rs.close()
    }

    executeSQL(user1Conn.createStatement(), s"drop function if exists myUDF")
    executeSQL(user2Conn.createStatement(), s"drop function if exists myUDF")
  }

  // fails with: Stream '/jars/gemfire1.strlen-stringlengthudf.jar' was not found.
  def testUDFWithQueries(): Unit = {
    // Build a jar with UDF
    val udf1Source = "public class StringLengthUDF implements " +
        "org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return (s.length() + 100); " +
        "} }"
    var classfile = SparkUtilsAccess.createUDFClass("StringLengthUDF", udf1Source)
    val jar1 = SparkUtilsAccess.createJarFile(Seq(classfile), "stringlengthudf.jar", true)

    val udf2Source = "public class StringifyIntUDF implements " +
        "org.apache.spark.sql.api.java.UDF1<Integer,String> {" +
        " @Override public String call(Integer s){ " +
        "               return (s + \" stringified\"); " +
        "} }"
    classfile = SparkUtilsAccess.createUDFClass("StringifyIntUDF", udf2Source)
    val jar2 = SparkUtilsAccess.createJarFile(Seq(classfile))

    var user1stmt = getConn(jdbcUser1).createStatement()

    // create table and run some queries
    val tabName = "udf_coltab"
    executeSQL(user1stmt, s"create table $tabName (id int, name string, address varchar(100)," +
        " contact string) using column")
    for (i <- 1001 to 3000) {
      executeSQL(user1stmt, s"insert into $tabName values ($i, 'name_$i', 'address_$i'," +
          s" '98$i-765')")
    }
    executeSQL(user1stmt, s"select count(*) from $tabName")
    executeSQL(user1stmt, s"select id, contact from $tabName where id > 2980")
    executeSQL(user1stmt, s"select name, address from $tabName where contact = '981500-765'")

    // create and execute udfs
    executeSQL(user1stmt, s"CREATE FUNCTION strlen AS " +
        s"StringLengthUDF returns Integer using jar '$jar1'")
    executeSQL(user1stmt, s"CREATE FUNCTION stringifyInt AS " +
        s"StringifyIntUDF returns String using jar '$jar2'")

    user1stmt.execute(s"select strlen('11223344')")
    var rs = user1stmt.getResultSet
    var rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getInt(1) == 108, s"Expected 108, found ${rs.getInt(1)}")
      }
      rs.close()
    }
    user1stmt.execute(s"select stringifyInt(11223344)")
    rs = user1stmt.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getString(1).equals("11223344 stringified"), s"Expected" +
            s" '11223344 stringified', found '${rs.getString(1)}'")
      }
      rs.close()
    }

    executeSQL(user1stmt, s"drop function if exists strlen")
    executeSQL(user1stmt, s"drop function if exists stringifyInt")

    // Now execute old queries again with new constants
    user1stmt.execute(s"select count(*) from $tabName")
    rs = user1stmt.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getInt(1) == 2000, s"Expected 2000 rows," +
            s" found ${rs.getInt(1)}")
      }
      rs.close()
    }
    user1stmt.execute(s"select id, contact from $tabName where id > 2985")
    rs = user1stmt.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getInt(1) > 2985)
      }
      rs.close()
    }
    assert(rows == 15)
    user1stmt.execute(s"select name, address from $tabName where contact = '9812000-765'")
    rs = user1stmt.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getString(1).equalsIgnoreCase("name_2000"))
        assert(rs.getString(2).equalsIgnoreCase("address_2000"))
      }
      rs.close()
    }

    // Execute some other queries too
    user1stmt.execute(s"select * from $tabName where id = 2333")
    rs = user1stmt.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getInt(1) == 2333)
        assert(rs.getString(2).equalsIgnoreCase(s"name_2333"))
        assert(rs.getString(3).equalsIgnoreCase(s"address_2333"))
        assert(rs.getString(4).equalsIgnoreCase(s"982333-765"))
      }
      rs.close()
    }
    user1stmt.execute(s"select id, contact, name from $tabName where id < 2500 " +
        s"and name like 'name_159%'")
    rs = user1stmt.getResultSet
    rows = 0
    if (rs ne null) {
      while (rs.next()) {
        rows += 1
        assert(rs.getInt(1) < 1600 && rs.getInt(1) > 1589)
        assert(rs.getString(2).contains(s"98159"))
        assert(rs.getString(3).contains(s"name_159"))
      }
      rs.close()
    }
    assert(rows == 10, s"Expected 10 rows found $rows")
  }

}

object SplitClusterDUnitSecurityTest extends SplitClusterDUnitTestObject {

  private val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    logInfo((productDir + "/sbin/start-all.sh") !!)
  }

  def stopSparkCluster(productDir: String): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    logInfo(s"Stopping spark cluster in $productDir/work")
    if (sparkContext != null) sparkContext.stop()
    logInfo((productDir + "/sbin/stop-all.sh") !!)
  }

  def bootExistingAuthModule(props: Properties): Unit = {
    val bootAuth = new SerializableRunnable() {
      override def run(): Unit = {
        val store = Misc.getMemStoreBootingNoThrow
        if (store ne null) {
          val authModule = store.getDatabase.getAuthenticationService
          if (authModule ne null) {
            val propNamesIter = props.stringPropertyNames().iterator()
            while (propNamesIter.hasNext) {
              val propName = propNamesIter.next()
              store.setBootProperty(propName, props.getProperty(propName))
            }
            authModule.boot(false, props)
          }
        }
      }
    }
    DistributedTestBase.invokeInEveryVM(bootAuth)
    bootAuth.run()
  }

  override def createTablesAndInsertData(tableType: String): Unit = {}

  override def createComplexTablesAndInsertData(props: Map[String, String]): Unit = {}

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean, props:
  Map[String, String]): Unit = {}

  override def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {}
}

