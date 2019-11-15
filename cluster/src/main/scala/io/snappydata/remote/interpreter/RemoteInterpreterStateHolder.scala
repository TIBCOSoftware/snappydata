/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.remote.interpreter

import java.io._
import java.lang.reflect.Method
import java.net.URLClassLoader

import com.gemstone.gemfire.internal.shared.StringPrintWriter
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.SparkContext
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SnappySession

import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IMain, LoopCommands, Results}

class RemoteInterpreterStateHolder(val connId: Long) {

  val sc: SparkContext = SparkContext.getOrCreate()
  val snappy = new SnappySession(sc)

  lazy val pw = new StringPrintWriter()
  lazy val strOpStream = new StringOutputStrem(pw)

  var intp: SparkILoop = createSparkILoop
  initIntp()

  val allInterpretedLinesForReplay: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()

  private def initIntp(): Unit = {
    intp.interpret("import org.apache.spark.sql.functions._")
    intp.interpret("org.apache.spark.sql.SnappySession")
    intp.bind("sc", sc)
    intp.bind("snappy", snappy)
    pw.reset()
  }


  lazy val commandMethod: Method = {
    val methods = this.intp.getClass.getSuperclass.getSuperclass.getDeclaredMethods
    val commandmethod = methods.find(_.getName == "colonCommand").getOrElse(
      throw new IllegalStateException("expected colonCommand to be there"))
    commandmethod.setAccessible(true)
    commandmethod
  }

  var incomplete = new mutable.StringBuilder()
  val resultBuffer = new mutable.ArrayBuffer[String]()

  def interpret(code: Array[String]): Array[String] = {
    return interpret(code, false)
  }

  def interpret(code: Array[String], replay: Boolean): Array[String] = {
    this.resultBuffer.clear()
    pw.reset()
    if (code != null && !code.isEmpty && code(0).trim.startsWith(":")) {
      return processCommand(code(0).tail)
    }
    scala.Console.setOut(strOpStream)
    val tmpsb = new StringBuilder
    tmpsb.append(incomplete.toString())
    incomplete.setLength(0)
    var i = 0
    var lastResult: Results.Result = Results.Success
    while(i < code.length && !(lastResult == Results.Error)) {
      val line = code(i)
      if (tmpsb.isEmpty) tmpsb.append(line)
      else tmpsb.append("\n" + line)
      if (replay) println(line)
      lastResult = intp.interpret(tmpsb.toString())
      if (!(lastResult == Results.Error) && !replay) {
        allInterpretedLinesForReplay += line
      }
      if (lastResult == Results.Success) tmpsb.clear()
      resultBuffer += pw.toString.stripLineEnd
      pw.reset()
      println()
      i += 1
    }
    // return empty. process command will do the needful
    if (replay) return Array.empty

    if (!(lastResult == Results.Incomplete)) {
      pw.reset()
      incomplete.setLength(0)
    } else {
      incomplete.append(tmpsb.toString())
      resultBuffer +=  "___INCOMPLETE___"
    }
    resultBuffer.toArray
  }

  def processCommand(command: String): Array[String] = {
    scala.Console.setOut(strOpStream)
    val result = commandMethod.invoke(this.intp, command)
    if (resultBuffer.isEmpty) {
      val output = pw.toString
      output.split("\n")
    } else {
      resultBuffer.toArray
    }
  }

  def createSparkILoop: SparkILoop = {
    val settings: Settings = new Settings
    var classpath: String = ""
    val paths: Seq[File] = currentClassPath()
    for (f <- paths) {
      if (classpath.length > 0) classpath += File.pathSeparator
      classpath += f.getAbsolutePath
    }
    val in = new BufferedReader(new StringReader(""))
    val intp = new RemoteILoop(pw, this)
    settings.classpath.value = classpath
    intp.settings = settings
    intp.createInterpreter()
    pw.reset()
    intp
  }

  def close(): Unit = {
    intp.clearExecutionWrapper()
    intp.close()
    strOpStream.close()
    pw.close()
    snappy.clear()
    incomplete.setLength(0)
    allInterpretedLinesForReplay.clear()
  }

  private def currentClassPath(): Seq[File] = {
    val paths = classPath(Thread.currentThread.getContextClassLoader)
    val cps = System.getProperty("java.class.path").split(File.pathSeparator)
    if (cps != null) {
      for (cp <- cps) {
        paths += new File(cp)
      }
    }
    paths
  }

  private def classPath(cl: ClassLoader): mutable.MutableList[File] = {
    val paths = new mutable.MutableList[File]
    if (cl == null) return paths
    cl match {
      case ucl: URLClassLoader =>
        val urls = ucl.getURLs
        if (urls != null) for (url <- urls) {
          paths += new File(url.getFile)
        }
      case _ =>
    }
    paths
  }

  def replayCmd(): Unit = {
    if (allInterpretedLinesForReplay.nonEmpty) {
      val copy = allInterpretedLinesForReplay.clone()
      resetCmd()
      interpret(copy.toArray, true)
    } else {
      println("Nothing to replay")
    }
  }

  def resetCmd(): Unit = {
    intp.clearExecutionWrapper()
    intp.close()
    pw.reset()
    intp = createSparkILoop
    initIntp()
    incomplete.setLength(0)
    allInterpretedLinesForReplay.clear()
  }

  def history(): Unit = {
    allInterpretedLinesForReplay.foreach(println(_))
  }

  class StringOutputStrem(val spw: StringPrintWriter) extends OutputStream {
    override def write(b: Int): Unit = {
      spw.write(b)
    }
  }
}

class RemoteILoop(spw: StringPrintWriter, intpHelper: RemoteInterpreterStateHolder)
  extends SparkILoop(null.asInstanceOf[BufferedReader], new PrintWriter(spw)) {

  /** Available commands */
  override def commands: List[LoopCommand] = serviceableCommands

  lazy val serviceableCommands: List[RemoteILoop.this.LoopCommand] = {
    val inheritedCommands = sparkStandardCommands.filterNot(
      cmd => RemoteILoop.notTBeInheritedCommandNames.contains(cmd.name))
    val implementedCommands = RemoteILoop.snappyOverrideImpls.map {
      case "replay" => LoopCommand.nullary(
        "replay", "rerun all the commands since the start", intpHelper.replayCmd)
      case "reset" => LoopCommand.nullary(
        "reset", "reset the interpreter state", intpHelper.resetCmd)
      case "history" => LoopCommand.nullary(
        "history", "shows the history of commands", intpHelper.history)
      case x => throw new IllegalArgumentException(s"did not expect command $x")
    }

    inheritedCommands ++ implementedCommands
  }
}

object RemoteILoop {
  private val notTBeInheritedCommandNames = Set(
    "h?", "edit", "line", "load", "paste", "power",
    "quit", "replay", "reset", "settings", "history")

  private val snappyOverrideImpls = Set("replay", "reset", "history")
}