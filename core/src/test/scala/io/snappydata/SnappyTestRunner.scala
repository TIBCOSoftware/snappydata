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

// scalastyle:off
import java.io._
import java.net.InetAddress
import java.sql.{Connection, DriverManager}
import java.util.regex.Pattern

import scala.language.postfixOps
import scala.sys.process._
import scala.util.parsing.json.JSON
import com.gemstone.gemfire.internal.AvailablePort
import org.apache.commons.io.FileUtils
import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.{BeforeAndAfterAll, FunSuite, Retries}
import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils

/**
 * Extensible Abstract test suite to test different shell based commands
 * like submit jobs, snappy shell, spark shell etc.
 * The output of each of the processes are captured and validated.
 *
 * Class extending can mix match different methods like SnappyShell,
 * Job to create a test case.
 */
abstract class SnappyTestRunner extends FunSuite
with BeforeAndAfterAll
with Serializable
with Logging with Retries {
// scalastyle:on

  var snappyHome = ""
  var localHostName = ""
  var currWorkingDir = ""
  private val commandOutput = "command-output.txt"

  // One can ovveride this method to pass other parameters like heap size.
  def servers: String = s"$localHostName\n$localHostName"

  def leads: String = s"$localHostName -jobserver.waitForInitialization=true\n"

  def snappyShell: String = s"$snappyHome/bin/snappy-sql"

  def sparkShell: String = s"$snappyHome/bin/spark-shell"

  def clusterSuccessString: String = "Distributed system now has 4 members"

  private val availablePort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)
  private  var locatorDirPath = ""

  implicit class X(in: Seq[String]) {
    def pipe(cmd: String): Stream[String] =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  override def beforeAll(): Unit = {
    snappyHome = System.getenv("SNAPPY_HOME")
    if (snappyHome == null) {
      throw new Exception("SNAPPY_HOME should be set as an environment variable")
    }
    localHostName = "localhost"
    currWorkingDir = System.getProperty("user.dir")
    val workDir = new File(s"$snappyHome/work")
    if (workDir.exists) {
      FileUtils.deleteDirectory(workDir)
    }
    startupCluster
  }

  override def afterAll(): Unit = {
    stopCluster
  }

  def stopCluster(): Unit = {
    executeProcess("snappyCluster", s"$snappyHome/sbin/snappy-stop-all.sh", Some(commandOutput))
    new File(s"$snappyHome/conf/servers").delete()
    new File(s"$snappyHome/conf/leads").delete()
    executeProcess("sparkCluster", s"$snappyHome/sbin/stop-all.sh", Some(commandOutput))
  }

  def startupCluster(): Unit = {
    val serverFile = new File(s"$snappyHome/conf/servers")
    new PrintWriter(serverFile) {
      write(servers)
      close()
    }
    serverFile.deleteOnExit()
    val leadFile = new File(s"$snappyHome/conf/leads")
    new PrintWriter(leadFile) {
      write(leads)
      close()
    }
    leadFile.deleteOnExit()

    val (out, _) = executeProcess("snappyCluster", s"$snappyHome/sbin/snappy-start-all.sh",
      Some(commandOutput))

    if (!out.contains(clusterSuccessString)) {
      throw new Exception(s"Failed to start Snappy cluster: " + out)
    }
    executeProcess("sparkCluster", s"$snappyHome/sbin/start-all.sh", Some(commandOutput))
  }

  // scalastyle:off println
  def executeProcess(name: String , command: String,
      outFile: Option[String] = None): (String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream

    val (out, err) = outFile match {
      case None => stdout -> stderr
      case Some(f) =>
        val writer = new BufferedOutputStream(new FileOutputStream(f, true))
        writer -> writer
    }
    val teeOut = new TeeOutputStream(out, new BufferedOutputStream(stdoutStream))
    val teeErr = new TeeOutputStream(err, new BufferedOutputStream(stderrStream))

    val stdoutWriter = new PrintStream(teeOut, true)
    val stderrWriter = new PrintStream(teeErr, true)

    val workDir = new File(s"$currWorkingDir/$name")
    if (!workDir.exists) {
      workDir.mkdir()
    }

    val code = Process(command, workDir, "SNAPPY_HOME" -> snappyHome,
      "PYTHONPATH" -> s"$snappyHome/python/lib/py4j-0.10.4-src.zip:$snappyHome/python") !
      ProcessLogger(stdoutWriter.println, stderrWriter.println)
    var stdoutStr = stdoutStream.toString
    if (out ne stdout) {
      out.close()
    }
    if (code != 0) {
      // add an exception to the output to force failure
      stdoutStr += s"\n***** Exit with Exception code = $code\n"
    }
    (stdoutStr, stderrStream.toString)
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

  def Job(jobClass: String, lead: String, jarPath: String,
      confs: Seq[String] = Nil): Unit = {

    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""

    val jobSubmit = s"$snappyHome/bin/snappy-job.sh submit --lead $lead --app-jar $jarPath"

    val jobStatus = s"$snappyHome/bin/snappy-job.sh status --lead $lead --job-id "

    val jobCommand: String = s"$jobSubmit --app-name " +
        s"${jobClass}_${System.currentTimeMillis()} --class $jobClass $confStr"

    val (out, err) = executeProcess("snappyCluster", jobCommand, Some(commandOutput))

    val jobSubmitStr = out

    val jsonStr = if (jobSubmitStr.charAt(2) == '{') jobSubmitStr.substring(2)
    else jobSubmitStr.substring(4)

    def json = JSON.parseFull(jsonStr)
    val jobID = json match {
      case Some(map: Map[_, _]) =>
        map.asInstanceOf[Map[String, Map[String, Any]]]("result")("jobId")
      case other => throw new Exception(s"bad result : $jsonStr")
    }
    logInfo("jobID " + jobID)

    var status = "RUNNING"
    while (status == "RUNNING") {
      Thread.sleep(3000)
      val statusCommand = s"$jobStatus $jobID"
      val (out, err) = executeProcess("snappyCluster", statusCommand, Some(commandOutput))

      val jobSubmitStatus = out

      def statusjson = JSON.parseFull(jobSubmitStatus)
      statusjson match {
        case Some(map: Map[_, _]) =>
          val v = map.asInstanceOf[Map[String, Any]]("status")
          logInfo("Current status of job: " + v)
          status = v.toString
        case other => "bad Result"
      }
    }

    println(s" Job $jobClass finished with status $status")
    if (status == "ERROR") {
      throw new Exception(s"Failed to Execute job $jobClass")
    }
  }

  private val exceptionPattern = Pattern.compile("[a-zA-Z0-9_]*exception",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private def checkException(output: String): Boolean = {
    val matcher = exceptionPattern.matcher(output)
    while (matcher.find()) {
      val exceptionStr = matcher.group()
      if (!exceptionStr.equals("NoSuchObjectException") &&
          !exceptionStr.equals("JDODataStoreException") &&
          !exceptionStr.contains("JDOException") &&
          !exceptionStr.equals("SQLNonTransientConnectionException") &&
          !exceptionStr.equals("SQLExceptionFactory") &&
          !exceptionStr.equals("getSQLException") &&
          !exceptionStr.equals("generateCsSQLException") &&
          !exceptionStr.equals("StandardException") &&
          !exceptionStr.equals("newException")) {
        println(s"***** FAIL due to $exceptionStr")
        return true
      }
    }
    false
  }

  def SparkSubmit(name: String, appClass: String,
                  master: Option[String],
                  confs: Seq[String] = Nil,
                  appJar: String): Unit = {

    val sparkHost = InetAddress.getLocalHost.getHostName
    val masterStr = master.getOrElse(s"spark://$sparkHost:7077")
    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    val classStr = if (appClass.isEmpty) "" else s"--class  $appClass"
    val sparkSubmit = s"$snappyHome/bin/spark-submit $classStr --master $masterStr $confStr $appJar"
    val (out, err) = executeProcess(name, sparkSubmit, Some(commandOutput))

    if (checkException(out) || checkException(err)) {
      throw new Exception(s"Failed to submit $appClass")
    }
  }

  def RunExample(name: String, exampleClas: String,
                 args: Seq[String] = Nil): Unit = {
    val argsStr = args.mkString(" ")
    val runExample = s"$snappyHome/bin/run-example $exampleClas $argsStr"
    val (out, err) = executeProcess(name, runExample, Some(commandOutput))

    if (checkException(out) || checkException(err)) {
      throw new Exception(s"Failed to run $exampleClas")
    }
  }

  def SparkShell(confs: Seq[String], options: String, scriptFile : String): Unit = {
    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    val shell = s"$sparkShell $confStr $options -i $scriptFile"
    val (out, err) = executeProcess("snappyCluster", shell, Some(commandOutput))
    if (checkException(out) || checkException(err)) {
      throw new Exception(s"Failed to run $shell")
    }
  }

  def SparkShell(confs: Seq[String], options: String,
      scalaStatements: Seq[String]): Unit = {
    val writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(commandOutput, true))))
    try {
      val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
      scalaStatements pipe s"$sparkShell $confStr $options" foreach (s => {
        writer.println(s)
        if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
          throw new Exception(s"Failed to run Scala statement")
        }
      })
    } finally {
      writer.close()
    }
  }

/*
  def withExceptionHandling[T](function: => T): T = {
    try {
      function
    } catch {
      case e: Exception => throw e
  }
*/

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
        ProcessLogger(stdoutWriter.println, stderrWriter.println)
    var stdoutStr = stdoutStream.toString
    if (code != 0) {
      // add an exception to the output to force failure
      stdoutStr += s"\n***** Exit with Exception code = $code\n"
    }
    (stdoutStr, stderrStream.toString)
  }
}
