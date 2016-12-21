/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.examples

import java.io._

import org.apache.commons.io.output.TeeOutputStream

import scala.language.postfixOps
import scala.sys.process.{Process, _}
import scala.util.parsing.json.JSON


/**
 * This class tests out all the classes in examples directory.
 */
object ExampleTester {

  def snappyShell = s"$snappyHome/bin/snappy-shell"

  implicit class X(in: Seq[String]) {
    def pipe(cmd: String) =
        cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  var snappyHome = ""
  var localHostName = ""
  var currWorkingDir = ""


  def main(args: Array[String]) {
    snappyHome = args(0)
    localHostName = java.net.InetAddress.getLocalHost().getHostName()
    currWorkingDir = System.getProperty("user.dir")
    try {
      startupCluster()
      val oldQuickStart = ExampleConf.oldQuickStart(snappyHome)
      oldQuickStart map (c => runExample(c))
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        throw ex
      }
    } finally {
      stopCluster
    }
  }


  def runExample(example: Example): Unit = {
    println(s"############################################")
    println(s"Executing ${example.name}")
    example match {
      case ex: Job => runJob(ex.jobClass)
      case snShell: SnappyShell => snShell.sqlCommand pipe snappyShell foreach (s => {
         println(s)
        if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
          throw new Exception(s"Failed to run Query")
        }
      })

      case submit: SparkSubmit => sparkSubmit(submit.appClass, submit.confs, submit.appJar)
      case _ =>
    }
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


  def stopCluster(): Unit = {
    executeProcess(s"$snappyHome/sbin/snappy-stop-all.sh")
    new File(s"$snappyHome/conf/servers").deleteOnExit()
    executeProcess(s"$snappyHome/sbin/stop-all.sh")
  }

  /**
   * Verify that the number of classes in example directory has corresponding entry in ExamplesConf.json
   */
  def verify: Unit = {

  }


  private def sparkSubmit(appClass: String, confs: Seq[String], appJar: String): Unit = {

    val confStr = if (confs.size > 0) confs.foldLeft("")((r, c) => s"$r --conf $c") else ""
    val classStr = if (appClass.isEmpty) "" else s"--class  $appClass"
    val sparkSubmit = s"$snappyHome/bin/spark-submit $classStr --master spark://$localHostName:7077 $confStr $appJar"
    val (out, err) = executeProcess(sparkSubmit)

    if (out.toLowerCase().contains("exception")) {
      throw new Exception(s"Failed to submit $appClass")
    }
  }

  private def runJob(jobClass: String): Unit = {

    val jobSubmit = s"$snappyHome/bin/snappy-job.sh submit --lead localhost:8090 --app-jar $snappyHome/examples/jars/quickstart.jar"

    val jobStatus = s"$snappyHome/bin/snappy-job.sh status --lead localhost:8090 --job-id "

    val jobCommand = s"$jobSubmit --app-name ${jobClass}_${System.currentTimeMillis()} --class $jobClass"

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

}
