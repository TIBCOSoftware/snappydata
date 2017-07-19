package io.snappydata.cluster

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.sql.DriverManager
import java.util.Properties

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.security.LdapTestServer
import com.pivotal.gemfirexd.security.SecurityTestUtils
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import io.snappydata.Constant
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}
import io.snappydata.test.util.TestException
import io.snappydata.util.TestUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SnappyContext}

import scala.language.{implicitConversions, postfixOps}
import scala.sys.process.Process
import scala.sys.process._

class SecureSplitClusterDUnitTest(s: String)
    extends DistributedTestBase(s)
        with SplitClusterDUnitTestBase
        with Serializable {

  private[this] var ldapProperties: Properties = new Properties()

  private val eColTab1 = "EMBEDDEDCOLTAB1"
  private val eColTab2 = "EMBEDDEDCOLTAB2"
  private val sColTab1 = "SMARTCOLTAB1"
  private val sColTab2 = "SMARTCOLTAB2"
  private val eRowTab1 = "EMBEDDEDROWTAB1"
  private val eRowTab2 = "EMBEDDEDROWTAB2"
  private val sRowTab1 = "SMARTROWTAB1"
  private val sRowTab2 = "SMARTROWTAB2"

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

  override def _testComplexTypesForColumnTables_SNAP643(): Unit = {}

  def _testTableFormChanges(): Unit = {}

  // test to make sure that stock spark-shell works with SnappyData core jar
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

  // grant and revoke select, insert, update and delete and check same from smart side.
  //
  // Create a thin connection from smart side and attempts to modify hive metastore should fail.
  //
  // Attempt to get a snappysession with invalid credentials should fail. Try with null and
  // empty, username and password combinations.
  //
  // Create tables with different flavours of sql
  //
  // Use APIs to perform DDLs and DMLs from smart side.
  // create, drop, alter tables; select, insert, update, delete rows
  //
  // udf install jar

  // create row and column table in embedded mode and select, insert, update, delete and
  // drop from smart side and vice versa.
  def testSQLOpsWithValidCredentials(): Unit = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)
    val conn = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    val stmt = conn.createStatement()

    // create row and column table in embedded mode
    SplitClusterDUnitTest.createTableUsingJDBC(eColTab1, "column", conn, stmt, Map.empty, false)
    SplitClusterDUnitTest.createTableUsingJDBC(eRowTab1, "row", conn, stmt, Map.empty, false)


    val snc = testObject.getSnappyContextForConnector(locatorClientPort, props)

    // insert
    snc.sql(s"insert into $eColTab1 values (10000, 'ten thousand', 1000.23)")
    snc.sql(s"insert into $eRowTab1 values (10000, 'ten thousand', 1000.23)")
    // select
    var count = snc.sql(s"select count(*) from $eColTab1").collect()(0).getLong(0)
    assert(count == 1, s"expected 1 rows but found $count in $eColTab1")
    count = snc.sql(s"select count(*) from $eRowTab1").collect()(0).getLong(0)
    assert(count == 1, s"expected 1 rows but found $count in $eRowTab1")

    // update
    snc.sql(s"update $eColTab1 set col1 = 5000 where col1 = 10000")
    snc.sql(s"update $eRowTab1 set col1 = 5000 where col1 = 10000")
    var col1 = snc.sql(s"select * from $eColTab1").collect()(0).get(0)
    assert(col1 == 5000, s"Update failed in $eColTab1, found value $col1")
    col1 = snc.sql(s"select * from $eRowTab1").collect()(0).get(0)
    assert(col1 == 5000, s"Update failed in $eRowTab1, found value $col1")

    // delete
    snc.sql(s"delete from $eColTab1 where col1 = 5000")
    snc.sql(s"delete from $eRowTab1 where col1 = 5000")

    var rows = snc.sql(s"select * from $eColTab1").collect().length
    assert(rows == 0, s"expected 0 rows but found $rows in $eColTab1")
    rows = snc.sql(s"select * from $eRowTab1").collect().length
    assert(rows == 0, s"expected 0 rows but found $rows in $eRowTab1")

    // insert from embedded side. Adds 1005 rows each
    SplitClusterDUnitTest.populateTable(eColTab1, conn)
    SplitClusterDUnitTest.populateTable(eRowTab1, conn)

    rows = snc.sql(s"select * from $eColTab1").collect().length
    assert(rows == 1005, s"expected 1005 rows but found $rows in $eColTab1")
    rows = snc.sql(s"select * from $eRowTab1").collect().length
    assert(rows == 1005, s"expected 1005 rows but found $rows in $eRowTab1")

    // drop
    snc.sql(s"drop table $eColTab1")
    snc.sql(s"drop table $eRowTab1")
    assert(!snc.sparkSession.catalog.tableExists(eColTab1), s"$eColTab1 not dropped")
    assert(!snc.sparkSession.catalog.tableExists(eRowTab1), s"$eRowTab1 not dropped")

    // vice versa
  }

  def _testGrantRevokeAndHiveModification(): Unit = {
  }

  def _testWithInvalidCredentials(): Unit = {
  }

  def _testAPIsWithValidCredentials(): Unit = {
  }

  def _testAPIsWithInvalidCredentials(): Unit = {
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

