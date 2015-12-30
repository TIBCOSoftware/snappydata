/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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

import com.gemstone.gemfire.internal.i18n.LocalizedStrings
import com.gemstone.gemfire.internal.{GemFireTerminateError, GemFireUtilLauncher}
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource
import com.pivotal.gemfirexd.internal.impl.tools.ij.utilMain
import com.pivotal.gemfirexd.internal.tools.ij
import com.pivotal.gemfirexd.tools.{GfxdAgentLauncher, GfxdDistributionLocator, GfxdUtilLauncher}
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.LocalizedMessages

/**
 * Launcher class encompassing snappy processes command lines.
 *
 * Created by soubhikc on 30/09/15.
 */
class SnappyUtilLauncher extends GfxdUtilLauncher {

  import SnappyUtilLauncher._

  protected override def getTypes() : java.util.Map[String, GemFireUtilLauncher#CommandEntry] = {
    val types = super.getTypes()

    types.put("server", new CommandEntry(classOf[ServerLauncher], LocalizedMessages.res.getTextMessage("UTIL_Server_Usage"), false))
    types.put("locator", new CommandEntry(classOf[LocatorLauncher], LocalizedMessages.res.getTextMessage("UTIL_Locator_Usage"), false))


    types.put("leader", new CommandEntry(classOf[LeaderLauncher],
      LocalizedMessages.res.getTextMessage("UTIL_Lead_Usage"), false))

    types.put(SCRIPT_NAME, new CommandEntry(classOf[ij], LocalizedMessages.res.getTextMessage("UTIL_SnappyShell_Usage"), false))

    types
  }

  override def invoke(args: Array[String]) = {
    super.invoke(args)
  }

  override def validateArgs (args: Array[String]) = {
    super.validateArgs(args)
  }

  override def scriptName(): String = {
    SCRIPT_NAME
  }
}


object SnappyUtilLauncher {

  private val SCRIPT_NAME: String = "snappy"
  private val GET_CANONICAL_PATH_ARG: String = "--get-canonical-path"

  /**
   * @see GemFireUtilLauncher#main(String[])
   **/
  def main(args: Array[String]) : Unit = {

    utilMain.setBasePrompt(SCRIPT_NAME)

    val launcher = new SnappyUtilLauncher()

    try {
      // no args will default to using ij
      if (args.length == 0) {
        launcher.invoke(Array(SCRIPT_NAME))
      }
      // short-circuit for the internal "--get-canonical-path" argument used by
      // script to resolve the full path including symlinks (#43722)
      else if (args.length == 2 && GET_CANONICAL_PATH_ARG.equals(args(0))) {
        try {
          System.out.println(new File(args(1)).getCanonicalPath())
        } catch {
          case ioe: IOException =>
              // in case of any exception print the given path itself
          System.out.println(args(1))
        }

        return;

      } else {
        launcher.validateArgs(args)
        launcher.invoke(args)
      }
    } catch {
      case term: GemFireTerminateError => System.exit(term.getExitCode())
      case re: RuntimeException =>
        // look for a GemFireTerminateError inside
        var cause = re.getCause()
        while (cause != null) {
          if (cause.isInstanceOf[GemFireTerminateError]) {
            System.exit(cause.asInstanceOf[GemFireTerminateError].getExitCode())
          }
          cause = cause.getCause()
        }
        throw re;

    }

  }

}