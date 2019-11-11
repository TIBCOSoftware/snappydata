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
import java.net.URLClassLoader

import com.gemstone.gemfire.internal.shared.StringPrintWriter
import org.apache.spark.SparkContext
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SnappySession

import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IMain, Results}

class RemoteInterpreterStateHolder(val connId: Long) {

  lazy val pw = new StringPrintWriter()
  lazy val strOpStream = new StringOutputStrem(pw)

  lazy val intp: SparkILoop = createSparkILoop
  intp.interpret("import org.apache.spark.sql.functions._")
  intp.interpret("org.apache.spark.sql.SnappySession")

  val sc = SparkContext.getOrCreate()
  val snappy = new SnappySession(sc)

  intp.bind("sc", sc)
  intp.bind("snappy", snappy)
  pw.reset()

  var incomplete = new mutable.StringBuilder()

  def interpret(code: Array[String]): Array[String] = {
    scala.Console.setOut(strOpStream)
    val tmpsb = new StringBuilder
    tmpsb.append(incomplete.toString())
    incomplete.setLength(0)
    var i = 0
    var lastResult: Results.Result = Results.Success
    while(i < code.length && !(lastResult == Results.Error)) {
      val line = code(i)
      if (tmpsb.isEmpty) tmpsb.append(line)
      else (tmpsb.append("\n" + line))
      lastResult = intp.interpret(tmpsb.toString())
      i += 1
    }

    var outputStr: String = null
    if (!(lastResult == Results.Incomplete)) {
      outputStr = pw.toString
      pw.reset()
      incomplete.setLength(0)
    } else {
      incomplete.append(tmpsb.toString())
      outputStr = pw.toString
      outputStr = outputStr + "\n" + "___INCOMPLETE___";
    }
    if (outputStr != null) outputStr.split("\n")
    else Array.empty
  }

  def createSparkILoop: SparkILoop = {
    val settings: Settings = new Settings
    var classpath: String = ""
    val paths: Seq[File] = currentClassPath
    for (f <- paths) {
      if (classpath.length > 0) classpath += File.pathSeparator
      classpath += f.getAbsolutePath
    }
    val in = new BufferedReader(new StringReader(""))
    val intp = new SparkILoop(null.asInstanceOf[BufferedReader], new PrintWriter(pw))
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
    if (cl.isInstanceOf[URLClassLoader]) {
      val ucl = cl.asInstanceOf[URLClassLoader]
      val urls = ucl.getURLs
      if (urls != null) for (url <- urls) {
        paths += new File(url.getFile)
      }
    }
    paths
  }

  private def echo(str: String) = pw.write(str)

  class StringOutputStrem(val spw: StringPrintWriter) extends OutputStream {
    override def write(b: Int): Unit = {
      spw.write(b)
    }

  }
}