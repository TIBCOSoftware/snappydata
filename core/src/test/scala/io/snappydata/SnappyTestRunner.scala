package io.snappydata

import java.io._


import org.apache.commons.io.FileUtils
import org.apache.commons.io.output.TeeOutputStream
import org.apache.spark.Logging
import org.scalatest.{Retries, BeforeAndAfterAll, FunSuite}

import scala.sys.process._
import scala.language.postfixOps
import scala.util.parsing.json.JSON

abstract class SnappyTestRunner extends FunSuite
with BeforeAndAfterAll
with Serializable
with Logging with Retries {

  var snappyHome = ""
  var localHostName = ""
  var currWorkingDir = ""

  def snappyShell = s"$snappyHome/bin/snappy-shell"

  implicit class X(in: Seq[String]) {
    def pipe(cmd: String) =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  override def beforeAll(): Unit = {
    snappyHome = System.getenv("SNAPPY_HOME")
    if (snappyHome isEmpty) {
      throw new Exception("SNAPPY_HOME should be set as an environment variable")
    }
    localHostName = java.net.InetAddress.getLocalHost().getHostName()
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
    executeProcess(s"$snappyHome/sbin/snappy-stop-all.sh")
    new File(s"$snappyHome/conf/servers").deleteOnExit()
    executeProcess(s"$snappyHome/sbin/stop-all.sh")
  }

  def startupCluster(): Unit = {
    new PrintWriter(s"$snappyHome/conf/servers") {
      write(s"$localHostName\n$localHostName")
      close
    }
    val (out, err) = executeProcess(s"$snappyHome/sbin/snappy-start-all.sh")

    if (!out.contains("Distributed system now has 4 members")) {
      throw new Exception(s"Failed to start Snappy cluster")
    }
    val (out1, err1) = executeProcess(s"$snappyHome/sbin/start-all.sh")
  }

  def executeProcess(command: String): (String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream

    val teeOut = new TeeOutputStream(stdout, new BufferedOutputStream(stdoutStream))
    val teeErr = new TeeOutputStream(stderr, new BufferedOutputStream(stderrStream))

    val stdoutWriter = new PrintStream(teeOut, true)
    val stderrWriter = new PrintStream(teeErr, true)

    Process(command, new File(s"$currWorkingDir"), "SNAPPY_HOME" -> snappyHome) !
      ProcessLogger(stdoutWriter.println, stderrWriter.println)
    (stdoutStream.toString, stderrStream.toString)
  }


  def SnappyShell(name: String, sqlCommand: Seq[String]): Unit = {
    sqlCommand pipe snappyShell foreach (s => {
      println(s)
      if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
        throw new Exception(s"Failed to run Query")
      }
    })
  }

  def Job(jobClass: String, lead: String, jarPath: String, confs: Seq[String] = Seq.empty[String]): Unit = {

    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""

    val jobSubmit = s"$snappyHome/bin/snappy-job.sh submit --lead $lead --app-jar $jarPath"

    val jobStatus = s"$snappyHome/bin/snappy-job.sh status --lead $lead --job-id "

    val jobCommand = s"$jobSubmit --app-name ${jobClass}_${System.currentTimeMillis()} --class $jobClass $confStr"

    val (out, err) = executeProcess(jobCommand)

    val jobSubmitStr = out

    val jsonStr = if (jobSubmitStr.charAt(2) == '{')
      jobSubmitStr.substring(2)
    else jobSubmitStr.substring(4)

    def json = JSON.parseFull(jsonStr)
    val jobID = json match {
      case Some(map: Map[String, Any]) =>
        map.get("result").get.asInstanceOf[Map[String, Any]].get("jobId").get
      case other => throw new Exception(s"bad result : $jsonStr")
    }
    println("jobID " + jobID)


    var status = "RUNNING"
    while (status == "RUNNING") {
      Thread.sleep(3000)
      val statusCommand = s"$jobStatus $jobID"
      val (out, err) = executeProcess(statusCommand)

      val jobSubmitStatus = out

      def statusjson = JSON.parseFull(jobSubmitStatus)
      statusjson match {
        case Some(map: Map[String, Any]) => {
          val v = map.get("status").get
          println("Current status of job: " + v)
          status = v.toString
        }
        case other => "bad Result"
      }
    }

    println(s" Job $jobClass finished with status $status")
    if (status == "ERROR") {
      throw new Exception(s"Failed to Execute job $jobClass")
    }
  }

  def SparkSubmit(name: String, appClass: String, confs: Seq[String], appJar: String): Unit = {

    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    val classStr = if (appClass.isEmpty) "" else s"--class  $appClass"
    val sparkSubmit = s"$snappyHome/bin/spark-submit $classStr --master spark://$localHostName:7077 $confStr $appJar"
    val (out, err) = executeProcess(sparkSubmit)

    if (out.toLowerCase().contains("exception")) {
      throw new Exception(s"Failed to submit $appClass")
    }
  }

  def RunExample(name: String, exampleClas: String): Unit = {
    val runExample = s"$snappyHome/bin/run-example $exampleClas"
    val (out, err) = executeProcess(runExample)

    if (out.toLowerCase().contains("exception")) {
      throw new Exception(s"Failed to run $exampleClas")
    }
  }
}
