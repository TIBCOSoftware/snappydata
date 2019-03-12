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

import java.io.{BufferedWriter, ByteArrayInputStream, File, FileFilter, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.util

import scala.sys.process._
import scala.language.postfixOps

import io.snappydata.test.dunit.DistributedTestBase
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{IOFileFilter, TrueFileFilter, WildcardFileFilter}

import org.apache.spark.{Logging, TestPackageUtils}

class CassandraSnappyDUnitTest(val s: String)
    extends ClusterManagerTestBase(s)
        with Logging {
  // scalastyle:off println

  private val snappyProductDir = System.getenv("SNAPPY_HOME")

  val scriptPath = s"$snappyProductDir/../../../cluster/src/test/resources/scripts"

  val currDir = System.getProperty("user.dir")

  val userHome = System.getProperty("user.home")

  var cassandraClusterLoc = ""

  private val commandOutput = "command-output.txt"

  def snappyShell: String = s"$snappyProductDir/bin/snappy-sql"

  override def beforeClass(): Unit = {

    logInfo(s"Starting snappy cluster in $snappyProductDir/work")

    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    val start = System.currentTimeMillis
    "curl -OL http://www-us.apache.org/dist/cassandra/2.1.21/apache-cassandra-2.1.21-bin.tar.gz".!!
    val end1 = System.currentTimeMillis
    logInfo("Time to download cassandra " + (end1 - start))
    logInfo("Cassandra jar downloaded")
    val jarLoc = getUserAppJarLocation("apache-cassandra-2.1.21-bin*", currDir)
    ("tar xvf " + jarLoc).!!
    logInfo("Cassandra jar Unpackaed")
    val cmd = Seq("find", currDir, "-name", "apache-cassandra-2.1.21", "-type", "d")
    val res = cmd.lineStream_!.toList
    logInfo("Cassandra folder location : " + res)
    cassandraClusterLoc = res.head
    (cassandraClusterLoc + "/bin/cassandra").!!
    logInfo("Starting cassandra cluster " + cassandraClusterLoc)
    val end = System.currentTimeMillis
    logInfo("Time to install and start cassandra cluster " + (end - start))

  }

  override def afterClass(): Unit = {

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)

    FileUtils.moveDirectory(new File(s"$snappyProductDir/work"), new File
    (s"$snappyProductDir/workTestCassandraSnappy"))

    logInfo("Stopping cassandra cluster")
    val p = Runtime.getRuntime.exec("pkill -f cassandra")
    p.waitFor()
    p.exitValue() == 0
    logInfo("Cassandra cluster stopped successfully")
  }

  def getJobJar(className: String, packageStr: String = ""): String = {
    val dir = new File(s"$snappyProductDir/../../../cluster/build-artifacts/scala-2.11/classes/"
        + s"scala/test/$packageStr")
    assert(dir.exists() && dir.isDirectory, s"snappy-cluster scala tests not compiled. Directory " +
        s"not found: $dir")
    val jar = TestPackageUtils.createJarFile(dir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getName.contains("CassandraSnappyConnectionJob")
      }
    }).toList, Some(packageStr))
    assert(!jar.isEmpty, s"No class files found for CassandraSnappyConnectionJob")
    jar
  }

  private def buildJobBaseStr(packageStr: String, className: String): String = {
    s"$snappyProductDir/bin/snappy-job.sh submit --app-name $className" +
        s" --class $packageStr.$className" +
        s" --app-jar ${getJobJar(className, packageStr.replaceAll("\\.", "/") + "/")}"
  }

  def submitAndVerifyJob(jobBaseStr: String, jobCmdAffix: String): Unit = {
    // Create config file with credentials

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
        consoleLog = s"$snappyProductDir/bin/snappy-job.sh status --job-id $jobId ".!!
        if (consoleLog.contains("FINISHED")) logInfo(s"Job $jobId completed. $consoleLog")
        consoleLog.contains("FINISHED")
      }

      override def description() = {
        logInfo(consoleLog)
        s"Job $jobId did not complete in time."
      }
    }
  }

  private def getJobId(str: String): String = {
    val idx = str.indexOf("jobId")
    str.substring(idx + 9, idx + 45)
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
    submitAndVerifyJob(buildJobBaseStr("io.snappydata.cluster", "CassandraSnappyConnectionJob"),
      "--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1" +
          " --conf spark.cassandra.connection.host=localhost")
  }

  def externalTableCreateTest(): Unit = {
    (cassandraClusterLoc + s"/bin/cqlsh -f $scriptPath/cassandra_script2").!!
    logInfo("deploy package and create external table from cassandra table")
    SnappyShell("CreateExternalTable",
      Seq("connect client 'localhost:1527';",
        "deploy package cassandraJar 'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7'" +
            s" path '$userHome/.ivy2';",
        "create external table customer2 using org.apache.spark.sql.cassandra" +
            " options (table 'customer1', keyspace 'test1'," +
            " spark.cassandra.input.fetch.size_in_rows '200000'," +
            " spark.cassandra.read.timeout_ms '10000');",
        "select * from customer2;",
        "exit;"))
  }
}
