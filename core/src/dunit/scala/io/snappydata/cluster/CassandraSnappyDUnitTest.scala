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
package io.snappydata.cluster

import java.io._
import java.nio.file.{Files, Paths}
import java.util

import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{IOFileFilter, TrueFileFilter, WildcardFileFilter}
import org.apache.spark.Logging

import scala.language.postfixOps
import scala.sys.process._

class CassandraSnappyDUnitTest(val s: String)
    extends DistributedTestBase(s)
        with Logging {
  // scalastyle:off println

  private val snappyProductDir = System.getenv("SNAPPY_HOME")

  val scriptPath = s"$snappyProductDir/../../../cluster/src/test/resources/scripts"

  val downloadLoc = s"$snappyProductDir/../distributions"

  val userHome = System.getProperty("user.home")

  val currDir = System.getProperty("user.dir")

  var cassandraClusterLoc = ""

  private val commandOutput = "command-output.txt"

  val port = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort

  def snappyShell: String = s"$snappyProductDir/bin/snappy-sql"

  override def beforeClass(): Unit = {

    super.beforeClass()
    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")

    val confDir = s"$snappyProductDir/conf"
    val sobj = new SplitClusterDUnitTest(s)
    sobj.writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort",
      s"$confDir/locators")
    sobj.writeToFile(s"localhost  -locators=localhost[$port]",
      s"$confDir/leads")
    sobj.writeToFile(s"""localhost  -locators=localhost[$port] -client-port=$netPort2
         |""".stripMargin, s"$confDir/servers")
    logInfo(s"Starting snappy cluster in $snappyProductDir/work")

    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)
    Thread.sleep(5000)

    val jarLoc = getLoc(downloadLoc)
    if(jarLoc.nonEmpty) {
      cassandraClusterLoc = jarLoc.head
    } else {
      ("curl -OL http://www-us.apache.org/dist/cassandra/" +
          s"2.1.21/apache-cassandra-2.1.21-bin.tar.gz").!!
      val jarLoc = getUserAppJarLocation("apache-cassandra-2.1.21-bin.tar.gz", currDir)
      ("tar xvf " + jarLoc).!!
      var loc = getLoc(currDir).head
      s"mv $loc $downloadLoc".!!
      cassandraClusterLoc = s"$downloadLoc/apache-cassandra-2.1.21"
    }
    (cassandraClusterLoc + "/bin/cassandra").!!
    logInfo("Cassandra cluster started")
  }

  override def afterClass(): Unit = {
    super.afterClass()

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)

    s"rm -rf $snappyProductDir/work".!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))

    logInfo("Stopping cassandra cluster")
    val p = Runtime.getRuntime.exec("pkill -f cassandra")
    p.waitFor()
    p.exitValue() == 0
    logInfo("Cassandra cluster stopped successfully")
  }

  def getLoc(path: String): List[String] = {
    val cmd = Seq("find", path, "-name", "apache-cassandra-2.1.21", "-type", "d")
    val res = cmd.lineStream_!.toList
    logInfo("Cassandra folder location : " + res)
    res
  }

  protected def getUserAppJarLocation(jarName: String, jarPath: String) = {
    var userAppJarPath: String = null
    if (new File(jarName).exists) jarName
    else {
      val baseDir: File = new File(jarPath)
      try {
        val filter: IOFileFilter = new WildcardFileFilter(jarName)
        val files: util.List[File] = FileUtils.listFiles(baseDir, filter,
          TrueFileFilter.INSTANCE).asInstanceOf[util.List[File]]
        logInfo("Jar file found: " + util.Arrays.asList(files))
        import scala.collection.JavaConverters._
        for (file1: File <- files.asScala) {
          if (!file1.getAbsolutePath.contains("/work/") ||
              !file1.getAbsolutePath.contains("/scala-2.10/")) {
            userAppJarPath = file1.getAbsolutePath
          }
        }
      }
      catch {
        case e: Exception =>
          logInfo("Unable to find " + jarName + " jar at " + jarPath + " location.")
      }
      userAppJarPath
    }
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
        writer.println(s)
        if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
          throw new Exception(s"Failed to run Query: $s")
        }
      })
    } finally {
      writer.close()
    }
  }

  def testDeployPackageWithCassandra(): Unit = {
    snappyJobTest()
    externalTableCreateTest()
  }

  def snappyJobTest(): Unit = {
    (cassandraClusterLoc + s"/bin/cqlsh -f $scriptPath/cassandra_script1").!!
    val obj = new SplitClusterDUnitSecurityTest(s)
    obj.submitAndVerifyJob(obj.buildJobBaseStr("io.snappydata.cluster",
      "CassandraSnappyConnectionJob"),
      "--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1" +
          " --conf spark.cassandra.connection.host=localhost")
    logInfo("Job completed")
  }

  def externalTableCreateTest(): Unit = {
    (cassandraClusterLoc + s"/bin/cqlsh -f $scriptPath/cassandra_script1").!!
    logInfo("deploy package and create external table from cassandra table")
    SnappyShell("CreateExternalTable",
      Seq(s"connect client 'localhost:$netPort';",
        "deploy package cassandraJar 'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7'" +
            s" path '$userHome/.ivy2';",
        "drop table if exists customer2;",
        "create external table customer2 using org.apache.spark.sql.cassandra" +
            " options (table 'customer', keyspace 'test'," +
            " spark.cassandra.input.fetch.size_in_rows '200000'," +
            " spark.cassandra.read.timeout_ms '10000');",
        "select * from customer2;",
        "exit;"))
  }
}
