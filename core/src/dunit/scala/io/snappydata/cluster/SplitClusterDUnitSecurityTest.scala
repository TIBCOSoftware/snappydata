/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.db.FabricDatabase
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.Constant
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, SerializableRunnable, VM}
import io.snappydata.util.TestUtils
import org.apache.commons.io.FileUtils

import org.apache.spark.TestPackageUtils
import org.apache.spark.sql.types.{IntegerType, StructField}
import org.apache.spark.sql.{Row, SnappyContext, SnappySession, TableNotFoundException}

class SplitClusterDUnitSecurityTest(s: String)
    extends DistributedTestBase(s)
        with SplitClusterDUnitTestBase
        with Serializable {

  private[this] var ldapProperties: Properties = new Properties()
  private var restartLdap = false;

  private val embeddedColTab1 = "EMBEDDEDCOLTAB1"
  private val smartColTab1 = "SMARTCOLTAB1"
  private val embeddedRowTab1 = "EMBEDDEDROWTAB1"
  private val smartRowTab1 = "SMARTROWTAB1"

  private[this] val bootProps: Properties = new Properties()
  bootProps.setProperty("log-file", "snappyStore.log")
  bootProps.setProperty("log-level", "config")
  bootProps.setProperty("statistic-archive-file", "snappyStore.gfs")
  bootProps.setProperty("spark.executor.cores", TestUtils.defaultCores.toString)
  System.setProperty(Constant.COMPRESSION_MIN_SIZE, compressionMinSize)

  var adminConn = null: Connection
  var user1Conn = null: Connection
  var user2Conn = null: Connection
  var snc = null: SnappyContext

  private[this] var host: Host = _
  var vm0: VM = _
  var vm1: VM = _
  var vm2: VM = _
  var vm3: VM = _

  val jdbcUser1 = "gemfire1"
  val jdbcUser2 = "gemfire2"
  val jdbcUser3 = "gemfire3"
  val adminUser1 = "gemfire4"

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

  private val snappyProductDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  private val jobConfigFile = s"$snappyProductDir/conf/job.config"

  protected val productDir =
    testObject.getEnvironmentVariable("APACHE_SPARK_HOME")

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
    writeToFile(
      s"localhost  -peer-discovery-port=$port -client-port=$netPort $compressionArg $ldapConf",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $waitForInit $compressionArg $ldapConf",
      s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort1 $compressionArg $ldapConf
          |localhost  -locators=localhost[$port] -client-port=$netPort2 $compressionArg $ldapConf
          |""".stripMargin, s"$confDir/servers")
    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    SplitClusterDUnitSecurityTest.startSparkCluster(productDir)
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
    SplitClusterDUnitSecurityTest.stopSparkCluster(productDir)

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
    SplitClusterDUnitTest.invokeSparkShell(snappyProductDir, locatorClientPort, props)
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
      Map("COLUMN_BATCH_SIZE" -> "50"), false)
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
        Map("COLUMN_BATCH_SIZE" -> "50"), false)
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
    if (setSNC) snc = testObject.getSnappyContextForConnector(locatorClientPort, props)
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
    var adminStmt = adminConn.createStatement()

    SplitClusterDUnitTest.createTableUsingJDBC(embeddedColTab1, "column", user1Conn, user1Stmt,
      Map("COLUMN_BATCH_SIZE" -> "50"))
    SplitClusterDUnitTest.createTableUsingJDBC(embeddedRowTab1, "row", user1Conn, user1Stmt)

    // All DMLs from another user should fail
    def assertFailure(sql: () => Unit, s: String): Unit = {
      try {
        sql()
        assert(false, s"Should have failed: $s")
      } catch {
        case sqle: SQLException =>
          if ("42502".equals(sqle.getSQLState) || "42500".equals(sqle.getSQLState)) {
            logInfo(s"Found expected error: $sqle")
          } else {
            logError(s"Found different SQLState: ${sqle.getSQLState}")
            throw sqle
          }
        case t: Throwable => if (t.getMessage.contains("42502")) {
          logInfo(s"Found expected error in: ${t.getClass.getName}, ${t.getMessage}")
        } else {
          logInfo(s"Found unexpected error in: ${t.getClass.getName}, ${t.getMessage}")
          throw t
        }
      }
    }

    def executeSQL(stmt: Statement, s: String): Unit = {
      stmt.execute(s)
      val rs = stmt.getResultSet
      if (rs ne null) {
        while (rs.next()) {}
        rs.close()
      }
    }

    val sqls = List(s"select * from $jdbcUser1.$embeddedColTab1",
      s"select * from $jdbcUser1.$embeddedRowTab1",
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

    verifyGrantRevoke("select", List(sqls(0), sqls(1)))
    verifyGrantRevoke("insert", List(sqls(2), sqls(3)))
    // No update on column tables
    verifyGrantRevoke("update", List(sqls(4), sqls(5)))
    // No delete on column tables
    verifyGrantRevoke("delete", List(sqls(6), sqls(7)))

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
    executeSQL(user2Stmt, sqls(3)) // insert into embeddedRowTab1
    snc.sql(sqls(3)).collect() // insert into embeddedRowTab1
    assertFailure(() => {executeSQL(user2Stmt, sqls(7))}, sqls(7)) // delete on embeddedRowTab1
    assertFailure(() => {snc.sql(sqls(6)).collect()}, sqls(6)) // delete on embeddedColTab1
  }

  def restartCluster(): Unit = {
    user1Conn.close()
    user2Conn.close()
    adminConn.close()
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

    sns.createTable(smartColTab1, "column", dataDF.schema, Map("COLUMN_BATCH_SIZE" -> "5"), false)
    sns.createTable(smartRowTab1, "row", dataDF.schema, Map.empty[String, String], false)
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

  def getJobJar(className: String, packageStr: String = ""): String = {
    val dir = new File(s"$snappyProductDir/../../../cluster/build-artifacts/scala-2.11/classes/"
        + s"test/$packageStr")
    assert(dir.exists() && dir.isDirectory, s"snappy-cluster scala tests not compiled. Directory " +
        s"not found: $dir")
    val jar = TestPackageUtils.createJarFile(dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("SecureJob")
      }
    }).toList, Some(packageStr))
    assert(!jar.isEmpty, s"No class files found for SecureJob")
    jar
  }

  def testSnappyJob(): Unit = {
    val jobBaseStr = buildJobBaseStr("io.snappydata.cluster", "SnappySecureJob")
    submitAndVerifyJob(jobBaseStr, s" --conf $opCode=sqlOps --conf $outputFile=SnappyValidJob.out")

    val colTab = "JOB_COLTAB"
    val rowTab = "JOB_ROWTAB"
    def submitJob(op: String): Unit = {
      val job = s"$jobBaseStr --conf $opCode=$op --conf $otherColTabName=$jdbcUser2.$colTab" +
          s" --conf $otherRowTabName=$jdbcUser2.$rowTab --conf $outputFile=Snappy${op}Job.out"
      logInfo(s"Submitting job $job")
      val consoleLog = job.!!
      logInfo(consoleLog)
      val jobId = getJobId(consoleLog)
      assert(consoleLog.contains("STARTED"), "Job not started")
      DistributedTestBase.waitForCriterion(getWaitCriterion(jobId), 60000, 500, true)
    }

    user2Conn = getConn(jdbcUser2, setSNC = true)
    val stmt = user2Conn.createStatement()
    SplitClusterDUnitTest.createTableUsingJDBC(colTab, "column", user2Conn, stmt,
      Map("COLUMN_BATCH_SIZE" -> "50"))
    SplitClusterDUnitTest.createTableUsingJDBC(rowTab, "row", user2Conn, stmt)

    submitJob("nogrant") // tells job to verify DMLs without any explicit grant

    Seq("select", "insert", "update", "delete").foreach(dml => {
      permitConn(stmt, "grant", dml, colTab, rowTab, jdbcUser1)
      submitJob(dml) // tells job to verify respective dml
      permitConn(stmt, "revoke", dml, colTab, rowTab, jdbcUser1)
    })

    Seq("select", "insert", "update", "delete").foreach(dml => {
      permit(snc.snappySession, "grant", dml, colTab, rowTab, jdbcUser1)
      submitJob(dml) // tells job to verify respective dml
      permit(snc.snappySession, "revoke", dml, colTab, rowTab, jdbcUser1)
    })

    // Submit the same job with invalid credentials
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "job.config"))
    writeToFile(s"-u $jdbcUser1:invalid", jobConfigFile)
    logInfo(s"Re-submitting job $jobBaseStr with invalid credentials.")
    val consoleLog = s"$jobBaseStr --conf $outputFile=SnappyInvalidJob.out".!!
    logInfo(consoleLog)
    assert(consoleLog.contains("The supplied authentication is invalid"), "Job should have failed")
  }

  def testSnappyStreamingJob(): Unit = {
    submitAndVerifyJob(buildJobBaseStr("io.snappydata.cluster", "SnappyStreamingSecureJob"),
      s" --stream --conf $opCode=sqlOps --conf $outputFile=SnappyStreamingValidJob.out")
  }

  def testSnappyJavaJob(): Unit = {
    submitAndVerifyJob(buildJobBaseStr("io.snappydata.cluster", "SnappyJavaSecureJob"),
      s" --conf $opCode=sqlOps --conf $outputFile=SnappyJavaValidJob.out")
  }

  def testSnappyJavaStreamingJob(): Unit = {
    submitAndVerifyJob(buildJobBaseStr("io.snappydata.cluster", "SnappyJavaStreamingSecureJob"),
      s" --stream --conf $opCode=sqlOps --conf $outputFile=SnappyJavaStreamingValidJob.out")
  }

  def submitAndVerifyJob(jobBaseStr: String, jobCmdAffix: String): Unit = {
    // Create config file with credentials
    writeToFile(s"-u $jdbcUser1:$jdbcUser1", jobConfigFile)

    val job = s"$jobBaseStr $jobCmdAffix"
    logInfo(s"Submitting job $job")
    val consoleLog = job.!!
    logInfo(consoleLog)
    val jobId = getJobId(consoleLog)
    assert(consoleLog.contains("STARTED"), "Job not started")

    val wc = getWaitCriterion(jobId)
    DistributedTestBase.waitForCriterion(wc, 60000, 1000, true)
  }

  private def getWaitCriterion(jobId: String): WaitCriterion = {
    new WaitCriterion {
      var consoleLog = ""
      override def done() = {
        consoleLog = (s"$snappyProductDir/bin/snappy-job.sh status --job-id $jobId " +
            s" --passfile $jobConfigFile").!!
        if (consoleLog.contains("FINISHED")) logInfo(s"Job $jobId completed. $consoleLog")
        consoleLog.contains("FINISHED")
      }
      override def description() = {
        logInfo(consoleLog)
        s"Job $jobId did not complete in time."
      }
    }
  }

  private def buildJobBaseStr(packageStr: String, className: String): String = {
    s"$snappyProductDir/bin/snappy-job.sh submit --app-name $className" +
        s" --class $packageStr.$className" +
        s" --app-jar ${getJobJar(className, packageStr.replaceAll("\\.", "/") + "/")}" +
        s" --passfile $jobConfigFile"
  }

  private def getJobId(str: String): String = {
    val idx = str.indexOf("jobId")
    str.substring(idx + 9, idx + 45)
  }

  def _testUDFAndProcs(): Unit = {
  }

  def _testConcurrentUsers(): Unit = {
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
          val authModule = FabricDatabase.getAuthenticationServiceBase
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

