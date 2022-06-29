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
package io.snappydata.tools

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.util

import com.gemstone.gemfire.internal.GemFireUtilLauncher.{CommandEntry, SCRIPT_NAME}
import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.gemstone.gemfire.internal.{GemFireTerminateError, GemFireUtilLauncher, GemFireVersion}
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.{LocalizedOutput, LocalizedResource}
import com.pivotal.gemfirexd.internal.impl.tools.ij.utilMain
import com.pivotal.gemfirexd.internal.tools.ij
import com.pivotal.gemfirexd.tools.GfxdUtilLauncher.GET_CANONICAL_PATH_ARG
import com.pivotal.gemfirexd.tools.internal.{JarTools, MiscTools}
import com.pivotal.gemfirexd.tools.{GfxdSystemAdmin, GfxdUtilLauncher}
import io.snappydata.LocalizedMessages
import io.snappydata.gemxd.{SnappyDataVersion, SnappySystemAdmin}
import jline.console.ConsoleReader
import org.apache.spark.sql.execution.columnar.impl.StoreCallback

import scala.util.Properties.{javaVersion, javaVmName, versionString}

/**
 * Launcher class encompassing snappy processes command lines.
 */
class SnappyUtilLauncher extends GfxdUtilLauncher {

  SnappyUtilLauncher.init()

  GfxdUtilLauncher.snappyStore = true
  ClientSharedUtils.setThriftDefault(true)

  SnappyDataVersion.loadProperties()

  // gfxd commands not applicable in snappy
  protected var snappy_removed_commands: Set[String] = Set[String](
    "agent", "encrypt-password", "upgrade-disk-store", "export-disk-store")

  protected override def getTypes: java.util.Map[String, CommandEntry] = {
    val types: java.util.Map[String, CommandEntry] = new util.LinkedHashMap[String, CommandEntry]()

    types.put("server", new CommandEntry(classOf[ServerLauncher],
      LocalizedMessages.res.getTextMessage("UTIL_Server_Usage"), false))
    types.put("locator", new CommandEntry(classOf[LocatorLauncher],
      LocalizedMessages.res.getTextMessage("UTIL_Locator_Usage"), false))
    types.put("leader", new CommandEntry(classOf[LeaderLauncher],
      LocalizedMessages.res.getTextMessage("UTIL_Lead_Usage"), false))

    types.put(SCRIPT_NAME, new CommandEntry(classOf[ij],
      LocalizedMessages.res.getTextMessage("UTIL_SnappyShell_Usage"), false))

    val commands = GfxdSystemAdmin.getValidCommands
    for (cmd <- commands) {
      if (!"help".equals(cmd) && !cmd.contains("locator") &&
          !snappy_removed_commands.contains(cmd)) {
        types.put(cmd, new GemFireUtilLauncher.CommandEntry(classOf[SnappySystemAdmin],
          LocalizedResource.getMessage("UTIL_" + cmd.replace('-', '_') + "_ShortDesc"), true))
      }
    }

    // MiscTools utilities
    val miscToolsIterator = MiscTools.getValidCommands.entrySet.iterator()
    while (miscToolsIterator.hasNext) {
      val entry = miscToolsIterator.next()
      types.put(entry.getKey, new CommandEntry(classOf[MiscTools],
        LocalizedMessages.res.getTextMessage(entry.getValue), true))
    }

    // JarTools utilities
    val jarToolsIterator = JarTools.getValidCommands.entrySet.iterator()
    while (jarToolsIterator.hasNext) {
      val entry = jarToolsIterator.next()
      types.put(entry.getKey, new CommandEntry(classOf[JarTools],
        LocalizedMessages.res.getTextMessage(entry.getValue), true))
    }

    types
  }

  override def invoke(args: Array[String]): Unit = {
    super.invoke(args)
  }

  override def validateArgs(args: Array[String]): Unit = {
    super.validateArgs(args)
  }

  override def scriptName(): String = {
    SCRIPT_NAME
  }
}


object SnappyUtilLauncher extends StoreCallback {

  val SNAPPY_SPARK_SHELL = "snappy-scala"

  var INTERPRETER_MODE = false

  var printNewWelcome = false
  init()

  private def init(): Unit = {
    SCRIPT_NAME = System.getenv("SNAPPY_SCRIPT_NAME") match {
      case s if (s eq null) || s.length == 0 => "snappy"
      case s => s
    }
    if (SNAPPY_SPARK_SHELL.equals(SCRIPT_NAME)) {
      System.setProperty("LAUNCHER_INTERPRETER_MODE", "true")
      printNewWelcome = true
      INTERPRETER_MODE = true
    }
  }

  /**
   * @see GemFireUtilLauncher#main(String[])
   */
  def main(args: Array[String]): Unit = {

    utilMain.setBasePrompt(SCRIPT_NAME)

    val launcher = new SnappyUtilLauncher()

    // TODO: this needs to be switched off because 'exec scala' executes
    // scala code and can have a lot of '!' which is treated special by the
    // console reader which we don't want.
    // Will see if there can be a better workaround. Couldn't find any though
    System.setProperty(ConsoleReader.JLINE_EXPAND_EVENTS, "false")

    try {
      // no args will default to using ij
      if (args.length == 0 || INTERPRETER_MODE) {
        if (INTERPRETER_MODE) {
          validateIntpArgs(args)
          launcher.setInitialCommands(
            SnappyUtilLauncher.CONNECT_STR, SnappyUtilLauncher.INITIAL_FILES_TO_RUN_STR)
        }
        launcher.invoke(Array(SCRIPT_NAME))
      }
      // short-circuit for the internal "--get-canonical-path" argument used by
      // script to resolve the full path including symlinks (#43722)
      else if (args.length == 2 && GET_CANONICAL_PATH_ARG.equals(args(0))) {
        // scalastyle:off println
        try {
          System.out.println(new File(args(1)).getCanonicalPath)
        } catch {
          case _: IOException =>
            // in case of any exception print the given path itself
            System.out.println(args(1))
        }
        // scalastyle:on println
      } else {
        launcher.validateArgs(args)
        launcher.invoke(args)
      }
    } catch {
      case term: GemFireTerminateError => System.exit(term.getExitCode)
      case re: RuntimeException =>
        // look for a GemFireTerminateError inside
        var cause = re.getCause
        while (cause != null) {
          cause match {
            case err: GemFireTerminateError => System.exit(err.getExitCode)
            case _ =>
          }
          cause = cause.getCause
        }
        throw re;
    }
  }

  val intpUsage = s"\nUsage:\n" +
    s"\n${SCRIPT_NAME} [OPTIONS]\n\n" +
    s"OPTIONS and Default values\n" +
    s"\n   -c LOCATOR_OR_SERVER_ENDPOINT  (default value is locahost:1527)\n" +
    s"\n   -u USERNAME                    (default value is APP)\n" +
    s"\n   -p PASSWORD                    (default value is APP)\n" +
    s"\n   -r SCALA_FILE_PATHS            (comma separated paths if multiple)\n" +
    s"\n   -h, --help                     (prints script usage)\n"


  private def validateArgs(options: Map[Symbol, Any]): Unit = {
    val hostport = options.get('connection) match {
      case Some(x) => x.toString
      case None => "localhost:1527"
    }
    val user = options.getOrElse('user, "APP")
    val passwd = options.getOrElse('password, "APP")
    CONNECT_STR = s"connect client '${hostport};user=${user};password=${passwd}'"
    val filesToRun = options.get('run) match {
      case Some(x) => val files = x.toString
        files.split(",").foreach(p => {
          val path = Paths.get(p)
          if (!Files.exists(path)) {
            println(s"File $path does not exists")
            System.exit(1)
          }
        })
        files
      case None => ""
    }
    if (filesToRun.nonEmpty) {
      INITIAL_FILES_TO_RUN_STR = filesToRun.toString
    }
  }

  // expected comma separated files
  private var INITIAL_FILES_TO_RUN_STR: String = null
  private var CONNECT_STR: String = null;

  private def validateIntpArgs(args: Array[String]): Unit = {
    val arglist = args.toList
    type ArgsMap = Map[Symbol, Any]

    def isSwitch(s : String) = (s(0) == '-')

    def nextOption(map : ArgsMap, list: List[String]) : ArgsMap = {

      list match {
        case Nil => map
        case ("-c" ) :: value :: tail =>
          nextOption(map ++ Map('connection -> value), tail)
        case ("-u") :: value :: tail =>
          nextOption(map ++ Map('user -> value), tail)
        case ("-p") :: value :: tail =>
          nextOption(map ++ Map('password -> value), tail)
        case ("-r") :: value :: tail =>
          nextOption(map ++ Map('run -> value), tail)
        case ("-h" | "--help") :: tail =>
          println(intpUsage)
          System.exit(0)
          Map.empty
        case option :: tail => println(s"\nBad option $option or its usage")
          println(intpUsage)
          System.exit(0)
          Map.empty
      }
    }

    val options = nextOption(Map(),arglist)
    if (printNewWelcome) printWelcomeEnterprise()
    validateArgs(options)
  }

  def printWelcomeEnterprise(): Unit = {
    val version = GemFireVersion.getProductVersion + " " + GemFireVersion.getProductReleaseStage
    import org.apache.spark.SPARK_VERSION
    // scalastyle:off println
    println()
    println("""Welcome to
    _____                               ____        __
   / ___/____  ____ _____  ____  __  __/ __ \____ _/ /_____ _
   \__ \/ __ \/ __ `/ __ \/ __ \/ / / / / / / __ `/ __/ __ `/
  ___/ / / / / /_/ / /_/ / /_/ / /_/ / /_/ / /_/ / /_/ /_/ /  version %s on Spark version %s
 /____/_/ /_/\__,_/ .___/ .___/\__, /_____/\__,_/\__/\__,_/
                 /_/   /_/    /____/""".format(version, SPARK_VERSION))
    val welcomeMsg = "\nUsing Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    println(welcomeMsg)
    println()
    println("Type in expressions to have them evaluated.\n")
    println("Spark context available as 'sc'")
    println("Snappy session available as 'snappysession'.\n")
    // scalastyle:on println
  }
}
