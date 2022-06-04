/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import java.io._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}

import scala.language.postfixOps
import scala.sys.process._

import io.snappydata.Constant
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase}
import org.apache.commons.io.FileUtils

import org.apache.spark.Logging

class CassandraSnappyDUnitTest(val s: String)
    extends DistributedTestBase(s) with ClusterUtils with SnappyJobTestSupport with Logging {

  def getConnection(netPort: Int): Connection =
    DriverManager.getConnection(s"${Constant.DEFAULT_THIN_CLIENT_URL}localhost:$netPort")

  val scriptPath = s"$snappyHomeDir/../../../cluster/src/test/resources/scripts"
  val downloadPath = s"$snappyHomeDir/../../../dist"

  private[this] val cassandraVersion = "2.1.22"
  private[this] val cassandraConnVersion = System.getenv("SPARK_CONNECTOR_VERSION") match {
    case null => "2.0.13"
    case v if v.startsWith("2.4") => "2.4.3"
    case v if v.startsWith("2.3") => "2.3.3"
    case _ => "2.0.13"
  }

  lazy val downloadLoc = {
    val path = if (System.getenv().containsKey("GRADLE_USER_HOME")) {
      Paths.get(System.getenv("GRADLE_USER_HOME"), "cassandraDist")
    } else {
      Paths.get(System.getenv("HOME"), ".gradle", "cassandraDist")
    }
    Files.createDirectories(path)
    path.toString
  }

  val userHome = System.getProperty("user.home")

  val currDir = System.getProperty("user.dir")

  var cassandraClusterLoc = ""
  var cassandraConnectorJarLoc = ""
  var sparkXmlJarPath = ""

  private val commandOutput = "command-output.txt"

  val port = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort

  def snappyShell: String = s"$snappyProductDir/bin/snappy-sql"

  override def beforeClass(): Unit = {
    super.beforeClass()

    // stop any previous cluster and cleanup data
    stopSnappyCluster()

    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")

    val confDir = s"$snappyProductDir/conf"
    val sobj = new SplitClusterDUnitTest(s)
    sobj.writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort",
      s"$confDir/locators")
    sobj.writeToFile(s"localhost  -locators=localhost[$port]",
      s"$confDir/leads")
    sobj.writeToFile(s"""localhost  -locators=localhost[$port] -client-port=$netPort2
         |""".stripMargin, s"$confDir/servers")
    startSnappyCluster()

    // Thread.sleep(10000)
    logInfo("Download Location : " + downloadLoc)

    logInfo(s"Creating $downloadPath")
    new File(downloadPath).mkdir()
    new File(snappyProductDir, "books.xml").createNewFile()
    sparkXmlJarPath = downloadURI("https://repo1.maven.org/maven2/com/databricks/" +
        "spark-xml_2.11/0.4.1/spark-xml_2.11-0.4.1.jar")
    val cassandraClusterDir = s"apache-cassandra-$cassandraVersion"
    val cassandraConnectorJar = s"spark-cassandra-connector_2.11-$cassandraConnVersion.jar"
    cassandraClusterLoc = s"$downloadLoc/$cassandraClusterDir"
    cassandraConnectorJarLoc = s"$downloadLoc/$cassandraConnectorJar"
    var downloadFiles = true
    if (Files.exists(Paths.get(cassandraClusterLoc))) {
      if (Files.exists(Paths.get(cassandraConnectorJarLoc))) {
        downloadFiles = false
      } else {
        FileUtils.deleteQuietly(new File(cassandraClusterLoc))
      }
    }
    if (downloadFiles) {
      val cassandraTarball = s"apache-cassandra-$cassandraVersion-bin.tar.gz"
      s"curl -OL http://www.apache.org/dist/cassandra/$cassandraVersion/$cassandraTarball".!!
      ("curl -OL https://repo1.maven.org/maven2/com/datastax/spark/" +
          s"spark-cassandra-connector_2.11/$cassandraConnVersion/$cassandraConnectorJar").!!
      ("tar xf " + cassandraTarball).!!
      Files.createDirectories(Paths.get(downloadLoc))
      val locDir = Paths.get(cassandraClusterDir)
      ClusterUtils.copyDirectory(locDir, locDir, Paths.get(cassandraClusterLoc))
      Files.move(Paths.get(cassandraConnectorJar), Paths.get(cassandraConnectorJarLoc),
        StandardCopyOption.REPLACE_EXISTING)
    }
    logInfo("CassandraClusterLocation : " + cassandraClusterLoc +
        " CassandraConnectorJarLoc : " + cassandraConnectorJarLoc)
    (cassandraClusterLoc + "/bin/cassandra").!!
    logInfo("Cassandra cluster started")
  }

  override def afterClass(): Unit = {
    super.afterClass()

    stopSnappyCluster()

    logInfo("Stopping cassandra cluster")
    val cmd = "pkill -f cassandra"
    val p = Runtime.getRuntime.exec(cmd)
    val msg = p.waitFor() match {
      case 0 => "Cassandra cluster stopped successfully"
      case exitCode => s"Failed to stop cassandra cluster with '$cmd' (exitCode=$exitCode)"
    }
    logInfo(msg)
  }

  private def downloadURI(url: String): String = {
    val jarName = url.split("/").last
    val jar = new File(downloadPath, jarName)
    if (!jar.exists()) {
      logInfo(s"Downloading $url ...")
      s"curl -OL $url".!!
      val cmd = s"find $currDir -name $jarName"
      logInfo(s"Executing $cmd")
      val tempPath = cmd.lineStream_!.toList
      val tempJar = new File(tempPath.head)
      assert(tempJar.exists(), s"Did not find $jarName at $tempPath")
      assert(tempJar.renameTo(jar), s"Could not move $jarName to $downloadPath")
    }
    jar.getAbsolutePath
  }

  implicit class X(in: Seq[String]) {
    def pipe(cmd: String): Stream[String] =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  def SnappyShell(name: String, sqlCommand: Seq[String]): Unit = {
    val writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(commandOutput, true))))
    try {
      sqlCommand pipe snappyShell foreach (s => {
        // scalastyle:off println
        writer.println(s)
        // scalastyle:on println
        if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
          throw new Exception(s"Failed to run Query: $s")
        }
      })
    } finally {
      writer.close()
    }
  }

  def getCount(rs: ResultSet): Int = {
    var count = 0
    if (rs ne null) {
      while (rs.next()) {
        count += 1
      }
      rs.close()
    }
    count
  }

  private var user1Conn: Connection = null
  private var stmt1: Statement = null

  def testDeployPackageWithCassandra(): Unit = {
    (cassandraClusterLoc + s"/bin/cqlsh -f $scriptPath/cassandra_script1").!!
    user1Conn = getConnection(netPort)
    stmt1 = user1Conn.createStatement()
    doTestDeployPackageWithExternalTable()
    doTestDeployJarWithExternalTable()
    doTestDeployJarWithSnappyJob()
    doTestDeployPackageWithSnappyJob()
    doTestPackageViaSnappyJobCommand()
    doTestDeployPackageWithExternalTableInSnappyShell()
    doTestSNAP_3120()
  }

  def doTestPackageViaSnappyJobCommand(): Unit = {
    logInfo("Running testPackageViaSnappyJobCommand")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
      "--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1" +
          " --conf spark.cassandra.connection.host=localhost")
    logInfo("Job completed")
  }

  def doTestDeployPackageWithExternalTableInSnappyShell(): Unit = {
    logInfo("Running testDeployPackageWithExternalTableInSnappyShell")
    SnappyShell("CreateExternalTable",
      Seq(s"connect client 'localhost:$netPort';",
        "deploy package cassandraJar " +
            s"'com.datastax.spark:spark-cassandra-connector_2.11:$cassandraConnVersion';",
        "drop table if exists customer2;",
        "create external table customer2 using org.apache.spark.sql.cassandra" +
            " options (table 'customer', keyspace 'test'," +
            " spark.cassandra.input.fetch.size_in_rows '200000'," +
            " spark.cassandra.read.timeout_ms '10000');",
        "select * from customer2;",
        "undeploy cassandraJar;",
        "exit;"))
  }

  def doTestDeployPackageWithExternalTable(): Unit = {
    logInfo("Running testDeployPackageWithExternalTable")
    stmt1.execute("deploy package cassandraJar " +
        s"'com.datastax.spark:spark-cassandra-connector_2.11:$cassandraConnVersion'")
    stmt1.execute("drop table if exists customer2")
    stmt1.execute("create external table customer2 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer2")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)

    stmt1.execute("undeploy cassandrajar")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    stmt1.execute("drop table if exists customer2")
    try {
      stmt1.execute("create external table customer2 using org.apache.spark.sql.cassandra options" +
          " (table 'customer', keyspace 'test', " +
          "spark.cassandra.input.fetch.size_in_rows '200000'," +
          " spark.cassandra.read.timeout_ms '10000')")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case sqle: SQLException if (sqle.getSQLState == "42000") &&
          sqle.getMessage.contains("Failed to find " +
              "data source: org.apache.spark.sql.cassandra") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
    stmt1.execute("deploy package cassandraJar " +
        s"'com.datastax.spark:spark-cassandra-connector_2.11:$cassandraConnVersion'")
    stmt1.execute("deploy package GoogleGSONAndAvro " +
        "'com.google.code.gson:gson:2.8.5,com.databricks:spark-avro_2.11:4.0.0' " +
        s"path '$snappyProductDir/testdeploypackagepath'")
    stmt1.execute("deploy package MSSQL 'com.microsoft.sqlserver:sqljdbc4:4.0'" +
        " repos 'https://clojars.org/repo/'")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 3)

    logInfo("Restarting the cluster for " +
        "CassandraSnappyDUnitTest.doTestDeployPackageWithExternalTable()")
    stopSnappyCluster(deleteData = false)
    startSnappyCluster()

    user1Conn = getConnection(netPort)
    stmt1 = user1Conn.createStatement()

    stmt1.execute("drop table if exists customer2")
    stmt1.execute("create external table customer2 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer2")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 3, s"After restart, packages expected 3," +
        s" found ${stmt1.getResultSet}")

    stmt1.execute("undeploy mssql")
    stmt1.execute("undeploy cassandrajar")
    stmt1.execute("undeploy googlegsonandavro")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)
  }

  def doTestDeployJarWithExternalTable(): Unit = {
    logInfo("Running testDeployJarWithExternalTable")
    stmt1.execute(s"deploy jar cassJar '$cassandraConnectorJarLoc'")
    stmt1.execute(s"deploy jar xmlJar '$sparkXmlJarPath'")
    stmt1.execute("drop table if exists customer3")
    stmt1.execute("create external table customer3 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer3")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 2)

    stmt1.execute("create external table books using com.databricks.spark.xml options" +
        s" (path '$snappyProductDir/books.xml')")

    logInfo("Restarting the cluster for " +
        "CassandraSnappyDUnitTest.doTestDeployJarWithExternalTable()")
    stopSnappyCluster(deleteData = false)
    startSnappyCluster()

    user1Conn = getConnection(netPort)
    stmt1 = user1Conn.createStatement()
    stmt1.execute("drop table if exists customer3")

    stmt1.execute("create external table customer4 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer4")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 2)

    stmt1.execute("undeploy cassJar")
    stmt1.execute("undeploy xmlJar")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    stmt1.execute("drop table if exists customer4")
    try {
      stmt1.execute("create external table customer5 using org.apache.spark.sql.cassandra options" +
          " (table 'customer', keyspace 'test', " +
          "spark.cassandra.input.fetch.size_in_rows '200000'," +
          " spark.cassandra.read.timeout_ms '10000')")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case sqle: SQLException if (sqle.getSQLState == "42000") &&
          sqle.getMessage.contains("Failed to find " +
              "data source: org.apache.spark.sql.cassandra") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
  }

  def doTestDeployJarWithSnappyJob(): Unit = {
    logInfo("Running testDeployJarWithSnappyJob")
    stmt1.execute(s"deploy jar cassJar '$cassandraConnectorJarLoc'")
    stmt1.execute("drop table if exists customer")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
          "--conf spark.cassandra.connection.host=localhost")
    stmt1.execute("select * from customer")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)

    stmt1.execute("undeploy cassJar")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    stmt1.execute("drop table if exists customer")
    try {
      submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
        "--conf spark.cassandra.connection.host=localhost")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case e: Exception if e.getMessage.contains("Job failed with result:") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
  }

  def doTestDeployPackageWithSnappyJob(): Unit = {
    logInfo("Running testDeployPackageWithSnappyJob")
    stmt1.execute("deploy package cassandraJar " +
        s"'com.datastax.spark:spark-cassandra-connector_2.11:$cassandraConnVersion'")
    stmt1.execute("drop table if exists customer")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
      "--conf spark.cassandra.connection.host=localhost")
    stmt1.execute("select * from customer")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)
    stmt1.execute("undeploy cassandraJar")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)
    stmt1.execute("drop table if exists customer")
    try {
      submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
        "--conf spark.cassandra.connection.host=localhost")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case e: Exception if e.getMessage.contains("Job failed with result:") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
  }

  def doTestSNAP_3120(): Unit = {
    logInfo("Running testSNAP_3120")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)
    stmt1.execute(s"deploy package MSSQL 'com.microsoft.sqlserver:sqljdbc4:4.0'" +
        s" repos 'https://clojars.org/repo/' path '$snappyProductDir/mssqlJar1'")
    try {
      stmt1.execute("deploy package MSSQL1 'com.microsoft.sqlserver:sqljdbc4:4.0'" +
          s" repos 'https://clojars.org/repo/' path '$snappyProductDir/mssqlJar';")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case sqle: SQLException if sqle.getSQLState == "38000" => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)
    var rs = stmt1.getResultSet
    rs.next()
    assert(rs.getString(1) == "mssql")

    stmt1.execute("undeploy MSSQL")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    stmt1.execute("deploy package MSSQL1 'com.microsoft.sqlserver:sqljdbc4:4.0'" +
        s" repos 'https://clojars.org/repo/' path '$snappyProductDir/mssqlJar';")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)
    rs = stmt1.getResultSet
    rs.next()
    assert(rs.getString(1) == "mssql1")

    stmt1.execute("undeploy MSSQL1")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)
  }
}
