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

import java.io.{PrintWriter, ByteArrayOutputStream, ByteArrayInputStream, File}

import scala.language.postfixOps
import scala.sys.process.{Process, _}
import scala.util.parsing.json.JSON


/**
 * This class tests out all the classes in examples directory.
 */
object ExampleTester {


  val jobSubmit = "./bin/snappy-job.sh submit --lead localhost:8090 --app-jar examples/jars/quickstart.jar"
  val jobStatus = "./bin/snappy-job.sh status --lead localhost:8090 --job-id "

  def snappyShell = s"$snappyHome/bin/snappy-shell"

  implicit class X(in: Seq[String]) {
    // Don't do the BAOS construction in real code.  Just for illustration.
    def pipe(cmd: String) =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  var snappyHome = ""
  val examples = ExampleConf.examples

  def main(args: Array[String]) {
    snappyHome = args(0)

    startupCluster()
    try {
      examples map (c => runMainExample(c))
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      stopCluster
    }

  }

  def startupCluster(): Unit = {
    val pb = Process(s"$snappyHome/sbin/snappy-start-all.sh", new File(s"$snappyHome"))
    val exitCode = pb !

  }

  def stopCluster(): Unit = {
    val pb = Process(s"$snappyHome/sbin/snappy-stop-all.sh", new File(s"$snappyHome"))
    val exitCode = pb !

  }

  /**
   * Verify that the number of classes in example directory has corresponding entry in ExamplesConf.json
   */
  def verify: Unit = {

  }


  def runSetup(setup: Setup): Unit = {
    setup match {
      case shell: SnappyShellSetup =>
        shell.sqlCommand pipe snappyShell foreach println
      case shell: BashShellSetup =>
      case nil => //do nothing
    }
  }

  def runMainExample(example: Example): Unit = {
    println(s"Executing ${example.name}")
    example match {
      case ex: Job => {
        runSetup(ex.setup)
        runJob(ex.jobClass)
      }
      case shell: BashShellSetup => {

      }
      case _ =>
    }
  }


  private def runJob(jobClass: String): Unit = {

    val jobCommand = s"$jobSubmit --app-name ${jobClass}_${System.currentTimeMillis()} --class $jobClass"

    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)

    val pb = Process(jobCommand, new File(s"$snappyHome"), "SNAPPY_HOME" -> snappyHome)
    pb ! (ProcessLogger(stdoutWriter.println, stderrWriter.println))

    stdoutWriter.flush
    stderrWriter.flush

    val jobSubmitStr = stdoutStream.toString
    println(s"job submit output $stdoutStream")
    stdoutStream.reset
    stderrStream.reset

    val jsonStr = if (jobSubmitStr.charAt(2) == '{')
      jobSubmitStr.substring(2)
    else jobSubmitStr.substring(4)

    def json = JSON.parseFull(jsonStr)
    val jobID = json match {
      case Some(map: Map[String, Any]) =>
        map.get("result").get.asInstanceOf[Map[String, Any]].get("jobId").get
      case other => "bad Result"
    }
    println("jobID " + jobID)

    var status = "RUNNING"
    try {
      while (status == "RUNNING") {
        Thread.sleep(3000)
        val statusCommand = s"$jobStatus $jobID"
        val pb = Process(statusCommand, new File(s"$snappyHome"), "SNAPPY_HOME" -> snappyHome)

        pb ! (ProcessLogger(stdoutWriter.println, stderrWriter.println))

        stdoutWriter.flush
        stderrWriter.flush

        val jobSubmitStatus = stdoutStream.toString
        println(s"job status output $stdoutStream")
        stdoutStream.reset
        stderrStream.reset

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
    } catch {
      case e: Exception => {
        stdoutWriter.close
        stderrWriter.close
      }
    }

    println(s" Job $jobClass finished with status $status")
    if (status == "ERROR") {
      throw new Exception(s"Failed to Execute job $jobClass")
    }
  }

}
