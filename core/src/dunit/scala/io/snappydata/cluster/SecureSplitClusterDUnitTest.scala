package io.snappydata.cluster

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.sql.SQLException
import java.util.Properties

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.security.LdapTestServer
import com.pivotal.gemfirexd.security.SecurityTestUtils
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}
import io.snappydata.util.TestUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StructField}
import org.apache.spark.sql.{Row, SnappyContext, TableNotFoundException}

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process.Process
import scala.sys.process._

class SecureSplitClusterDUnitTest(s: String)
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

  private[this] var host: Host = _
  var vm0: VM = _
  var vm1: VM = _
  var vm2: VM = _
  var vm3: VM = _

  if (Host.getHostCount > 0) {
    host = Host.getHost(0)
    vm0 = host.getVM(0)
    vm1 = host.getVM(1)
    vm2 = host.getVM(2)
    vm3 = host.getVM(3)
  }

  val jdbcUser1 = "gemfire1"
  val jdbcUser2 = "gemfire2"
  val adminUser1 = "gemfire3"

  override def setUp(): Unit = {
    super.setUp()
  }

  override def tearDown2(): Unit = {
    super.tearDown2()
    if (restartLdap) {
      restartLdap = false
      stopLdapTestServer()
      setSecurityProps()
    }
  }

  def setSecurityProps(): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE}
    ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0,
      adminUser1, getClass.getResource("/auth.ldif").getPath)
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
  }

  def startArgs: Array[AnyRef] = Array(
    SecureSplitClusterDUnitTest.locatorPort, bootProps).asInstanceOf[Array[AnyRef]]

  private val snappyProductDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  protected val productDir =
    testObject.getEnvironmentVariable("APACHE_SPARK_HOME")

  override def locatorClientPort: Int = { SecureSplitClusterDUnitTest.locatorNetPort }

  override def startNetworkServers(): Unit = {}

  override protected def testObject = SecureSplitClusterDUnitTest

  override def beforeClass(): Unit = {
    super.beforeClass()

    setSecurityProps()

    logInfo(s"Starting snappy cluster in $snappyProductDir/work")
    // create locators, leads and servers files
    val port = SecureSplitClusterDUnitTest.locatorPort
    val netPort = SecureSplitClusterDUnitTest.locatorNetPort
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val confDir = s"$snappyProductDir/conf"
    val ldapConf = getLdapConf()
    writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort $ldapConf",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $ldapConf", s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort1 $ldapConf
          |localhost  -locators=localhost[$port] -client-port=$netPort2 $ldapConf
          |""".stripMargin, s"$confDir/servers")
    (snappyProductDir + "/sbin/snappy-start-all.sh").!!

    vm3.invoke(getClass, "startSparkCluster", productDir)
  }

  def getLdapConf(): String = {
    var conf = ""
    for (k <- List(Attribute.AUTH_PROVIDER, Attribute.USERNAME_ATTR, Attribute.PASSWORD_ATTR)) {
      conf += s"-$k=${ldapProperties.getProperty(k)} "
    }
    for (k <- List(AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      conf += s"-J-D$k=${ldapProperties.getProperty(k)} "
    }
    conf
  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm3.invoke(getClass, "stopSparkCluster", productDir)

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))

    Files.deleteIfExists(Paths.get(snappyProductDir, "work", "localhost-lead-1"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "work", "localhost-server-1"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "work", "localhost-server-2"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "work", "localhost-locator-1"))

    stopLdapTestServer()
  }

  def stopLdapTestServer(): Unit = {
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
  }

  private def writeToFile(str: String, fileName: String): Unit = {
    val pw = new PrintWriter(fileName)
    try {
      pw.write(str)
    } finally {
      pw.close()
    }
  }

  override def testColumnTableCreation(): Unit = {}

  override def testRowTableCreation(): Unit = {}

  // Test to make sure that stock spark-shell works with SnappyData core jar
  def testSparkShell(): Unit = {
    // perform some operation thru spark-shell
    val jars = Files.newDirectoryStream(Paths.get(s"$snappyProductDir/../distributions/"),
      "snappydata-core*.jar")
    val snappyDataCoreJar = jars.iterator().next().toAbsolutePath.toString
    // SparkSqlTestCode.txt file contains the commands executed on spark-shell
    val scriptFile: String = getClass.getResource("/SparkSqlTestCode.txt").getPath
    val sparkShellCommand = productDir + "/bin/spark-shell  --master local[3]" +
        s" --conf spark.snappydata.connection=localhost:$locatorClientPort" +
        s" --conf spark.snappydata.store.user=$jdbcUser1" +
        s" --conf spark.snappydata.store.password=$jdbcUser1" +
        s" --jars $snappyDataCoreJar" +
        s" -i $scriptFile"

    val cwd = new java.io.File("spark-shell-out")
    FileUtils.deleteQuietly(cwd)
    cwd.mkdirs()
    logInfo(s"about to invoke spark-shell with command: $sparkShellCommand in $cwd")

    Process(sparkShellCommand, cwd).!!
    FileUtils.deleteQuietly(cwd)

    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    val conn = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    val stmt = conn.createStatement()

    // accessing tables created thru spark-shell
    val rs1 = stmt.executeQuery("select count(*) from coltable")
    rs1.next()
    assert(rs1.getInt(1) == 5)

    val rs2 = stmt.executeQuery("select count(*) from rowtable")
    rs2.next()
    assert(rs2.getInt(1) == 5)
  }

  /**
    * Create row and column tables in embedded mode. Perform select, insert, update, delete and
    * drop from smart side and vice versa.
    */
  def testSQLOpsWithValidCredentials(): Unit = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    val conn = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    val stmt = conn.createStatement()
    val value = "brought up to zero"
    val snc = testObject.getSnappyContextForConnector(locatorClientPort, props)

    try {
      // Create row and column tables in embedded mode
      SplitClusterDUnitTest.createTableUsingJDBC(embeddedColTab1, "column", conn, stmt, Map
      ("COLUMN_BATCH_SIZE" -> "50"), false)
      SplitClusterDUnitTest.createTableUsingJDBC(embeddedRowTab1, "row", conn, stmt, Map.empty, false)

      // insert
      snc.sql(s"insert into $embeddedColTab1 values (10000, 'ten thousand', 1000.23)")
      snc.sql(s"insert into $embeddedRowTab1 values (10000, 'ten thousand', 1000.23)")
      // select
      var count = snc.sql(s"select count(*) from $embeddedColTab1").collect()(0).getLong(0)
      assert(count == 1, s"expected 1 rows but found $count in $embeddedColTab1")
      count = snc.sql(s"select count(*) from $embeddedRowTab1").collect()(0).getLong(0)
      assert(count == 1, s"expected 1 rows but found $count in $embeddedRowTab1")

      // update TODO do for column tables when support comes in master.
      snc.sql(s"update $embeddedRowTab1 set col1 = 5000 where col1 = 10000")
      var col1 = snc.sql(s"select * from $embeddedRowTab1").collect()(0).get(0)
      assert(col1 == 5000, s"Update failed in $embeddedRowTab1, found value $col1")

      // delete TODO do for column tables when support comes in master.
      snc.sql(s"delete from $embeddedRowTab1 where col1 = 5000")
      var rows = snc.sql(s"select * from $embeddedRowTab1").collect().length
      assert(rows == 0, s"expected 0 rows but found $rows in $embeddedRowTab1")

      // insert more data. Adds 1005 rows each
      SplitClusterDUnitTest.populateTable(embeddedColTab1, conn)
      SplitClusterDUnitTest.populateTable(embeddedRowTab1, conn)

      rows = snc.sql(s"select * from $embeddedColTab1").collect().length
      // 1005 + 1 (which was not deleted above)
      assert(rows == 1006, s"expected 1005 rows but found $rows in $embeddedColTab1")
      rows = snc.sql(s"select * from $embeddedRowTab1").collect().length
      assert(rows == 1005, s"expected 1005 rows but found $rows in $embeddedRowTab1")

      // drop
      snc.sql(s"drop table $embeddedColTab1")
      snc.sql(s"drop table $embeddedRowTab1")
      assert(!snc.sparkSession.catalog.tableExists(embeddedColTab1), s"$embeddedColTab1 not dropped")
      assert(!snc.sparkSession.catalog.tableExists(embeddedRowTab1), s"$embeddedRowTab1 not dropped")

      // vice versa: Create row and column table in smart connector mode
      snc.sql(s"create table $smartColTab1 (col1 INT, col2 STRING, col3 DECIMAL) using column")
      snc.sql(s"create table $smartRowTab1 (col1 INT, col2 STRING, col3 DECIMAL) using row")

      // insert. Adds 1005 rows each
      SplitClusterDUnitTest.populateTable(smartColTab1, conn)
      SplitClusterDUnitTest.populateTable(smartRowTab1, conn)

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
      // TODO do for column tables when support comes in master.
      stmt.execute(s"update $smartRowTab1 set col1 = 0, col2 = '$value' where " +
          s"col1 < 0")
      checkCol1(s"select * from $smartRowTab1 where col2 = '$value'")

      // delete TODO do for column tables when support comes in master.
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
      conn.close()
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

  /**
    * Grant and revoke select, insert, update and delete operations and verify from smart and
    * embedded side.
    *
    * Attempt to modify hive metastore should fail via a thin connection.
    */
  def testGrantRevokeAndHiveModification(): Unit = {
    var props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    val connUser1 = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    val stmtUser1 = connUser1.createStatement()
    val value = "brought up to zero"

    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser2)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser2)
    var connUser2 = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    var stmtUser2 = connUser2.createStatement()

    var sncUser2 = testObject.getSnappyContextForConnector(locatorClientPort, props)

    try {
      SplitClusterDUnitTest.createTableUsingJDBC(embeddedColTab1, "column", connUser1, stmtUser1,
        Map("COLUMN_BATCH_SIZE" -> "50"))
      SplitClusterDUnitTest.createTableUsingJDBC(embeddedRowTab1, "row", connUser1, stmtUser1)

      // All DMLs from another user should fail
      def assertFailure(sql: () => Unit, s: String): Unit = {
        try {
          sql()
          assert(false, s"Should have failed: $s")
        } catch {
          case e: SQLException =>
            if ("42502".equals(e.getSQLState) || "42500".equals(e.getSQLState))
              logInfo(s"Found expected error: $e")
            else {
              logInfo(s"Found different SQLState ${e.getSQLState}")
              throw e
            }
        }
      }

      List(s"select * from $jdbcUser1.$embeddedColTab1",
        s"select * from $jdbcUser1.$embeddedRowTab1",
        s"insert into $jdbcUser1.$embeddedColTab1 values (1, '$jdbcUser2', 1.1)",
        s"insert into $jdbcUser1.$embeddedRowTab1 values (1, '$jdbcUser2', 1.1)",
        s"update $jdbcUser1.$embeddedRowTab1 set col1 = 0, col2 = '$value by $jdbcUser2' where " +
            s"col1 < 0",
        s"delete from $jdbcUser1.$embeddedRowTab1 where col1 = 0"
      ).foreach(s => assertFailure(() => {stmtUser2.execute(s)}, s))

      def reset(): Unit = {
        // Get a fresh conn.
        // TODO May not be needed later when cached plans are cleared for grant/revoke
        stmtUser2.close()
        connUser2.close()
        connUser2 = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
        stmtUser2 = SplitClusterDUnitTest.getConnection(locatorClientPort, props).createStatement()

        sncUser2 = testObject.getSnappyContextForConnector(locatorClientPort, props)
      }

      def exe(permit: String, op: String): Unit = {
        val toFrom = if (permit.equalsIgnoreCase("grant")) "to" else "from"
        stmtUser1.execute(s"$permit $op on table $embeddedColTab1 $toFrom $jdbcUser2")
        stmtUser1.execute(s"$permit $op on table $embeddedRowTab1 $toFrom $jdbcUser2")
        if (!op.equalsIgnoreCase("select")) {
          // We need select permission for insert, update or delete operation
          stmtUser1.execute(s"$permit select on table $embeddedColTab1 $toFrom $jdbcUser2")
          stmtUser1.execute(s"$permit select on table $embeddedRowTab1 $toFrom $jdbcUser2")
        }
      }

      def verifyGrantRevoke(op: String, sqls: List[String]): Unit = {
        // grant
        reset()
        exe("grant", op)
        sqls.foreach(s => stmtUser2.execute(s))
        sqls.foreach(s => sncUser2.sql(s))

        // revoke
        reset()
        exe("revoke", op)
        sqls.foreach(s => assertFailure(() => {stmtUser2.execute(s)}, s))
        sqls.foreach(s => assertFailure(() => {sncUser2.sql(s)}, s))
      }

      verifyGrantRevoke("select", List(s"select * from $jdbcUser1.$embeddedColTab1",
        s"select * from $jdbcUser1.$embeddedRowTab1"))
      verifyGrantRevoke("insert",
        List(s"insert into $jdbcUser1.$embeddedColTab1 values (1, '$jdbcUser2', 1.1)",
          s"insert into $jdbcUser1.$embeddedRowTab1 values (1, '$jdbcUser2', 1.1)"))
      // No update on column tables
      verifyGrantRevoke("update", List(s"update $jdbcUser1.$embeddedRowTab1 set col1 = 0, col2 = " +
          s"'$value by $jdbcUser2' where col1 < 0"))
      // No delete on column tables
      verifyGrantRevoke("delete", List(s"delete from $jdbcUser1.$embeddedRowTab1 where col1 = 0"))

      // SNAPPY_HIVE_METASTORE should not be modifiable by users.
      val sql = s"insert into ${Misc.SNAPPY_HIVE_METASTORE}.VERSION values (1212, 'NA', 'NA')"
      assertFailure(() => {stmtUser1.execute(sql)}, sql)
    } finally {
      sncUser2.sparkContext.stop()
      stmtUser1.close()
      stmtUser2.close()
      connUser1.close()
      connUser2.close()
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

    val sns = testObject.getSnappyContextForConnector(locatorClientPort, props).snappySession

    try {
      val rdd = sns.sparkContext.parallelize(
        (1 to 113999).map(i => Data2(i, s"name_$i", s"address_$i")))
      val dataDF = sns.createDataFrame(rdd)
      val col1 = "COL1"
      val col2 = "COL2"
      val col3 = "COL3"
      val col4 = "COL4"

      sns.createTable(smartColTab1, "column", dataDF.schema, Map("COLUMN_BATCH_SIZE"->"50"), false)
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

      sns.sqlContext.alterTable(smartRowTab1, true, StructField(col4, IntegerType, true))

      sns.dropTable(smartColTab1, false)
      assertTableDeleted(() => {sns.catalog.refreshTable(smartColTab1)}, smartColTab1)
      sns.dropTable(smartRowTab1, false)
      assertTableDeleted(() => {sns.catalog.refreshTable(smartRowTab1)}, smartRowTab1)
    } finally {
      sns.sparkContext.stop()
    }
  }

  def _testUDFAndProcs(): Unit = {
  }

  def _testConcurrentUsers(): Unit = {
  }
}

object SecureSplitClusterDUnitTest extends SplitClusterDUnitTestObject {

  private val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    (productDir + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster(productDir: String): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    logInfo(s"Stopping spark cluster in $productDir/work")
    if (sparkContext != null) sparkContext.stop()
    (productDir + "/sbin/stop-all.sh") !!
  }

  override def createTablesAndInsertData(tableType: String): Unit = {}

  override def createComplexTablesAndInsertData(props: Map[String, String]): Unit = {}

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean, props:
  Map[String, String]): Unit = {}

  override def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {}
}

