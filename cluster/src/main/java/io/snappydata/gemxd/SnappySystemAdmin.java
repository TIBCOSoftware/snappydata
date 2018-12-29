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
package io.snappydata.gemxd;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.GemFireTerminateError;
import com.gemstone.gemfire.internal.GemFireUtilLauncher;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource;
import com.pivotal.gemfirexd.internal.impl.tools.ij.utilMain;
import com.pivotal.gemfirexd.tools.GfxdSystemAdmin;

public class SnappySystemAdmin extends GfxdSystemAdmin {

  private SnappySystemAdmin() {
    super();
    UTIL_Tools_DSProps = "UTIL_Snappy_Tools_DSProps";
    UTIL_DSProps_HelpPost = "UTIL_Snappy_Tools_DSProps_HelpPost";
  }

  public static void main(String[] args) {
    try {
      SnappyDataVersion.loadProperties();

      final SnappySystemAdmin admin = new SnappySystemAdmin();
      admin.initHelpMap();
      admin.initUsageMap();
      admin.initMapsForGFXD();
      for (String removedCmd : removedCommands) {
        admin.usageMap.remove(removedCmd);
        admin.helpMap.remove(removedCmd);
      }
      for (Map.Entry<String, String> overrideUse : modifiedUsageInfo
          .entrySet()) {
        admin.usageMap.put(overrideUse.getKey(), overrideUse.getValue());
      }
      for (Map.Entry<String, String> overrideHelp : modifiedHelpInfo
          .entrySet()) {
        admin.helpMap.put(overrideHelp.getKey(), overrideHelp.getValue());
      }

      admin.invoke(args);
    } catch (GemFireTerminateError term) {
      System.exit(term.getExitCode());
    }
  }

  @Override
  protected void printProductDirectory() {
    String productDirMessage = LocalizedResource.getMessage(
        "UTIL_version_ProductDirectory", getProductDir());
    System.out.println(utilMain.convertGfxdMessageToSnappy(productDirMessage));
  }

  @Override
  protected String getUsageString(String cmd) {
    return GemFireUtilLauncher.SCRIPT_NAME + ' ' +
        this.usageMap.get(cmd.toLowerCase());
  }

  @Override
  public void invoke(String[] args) {
    this.defaultLogFileName = null;
    try {
      if (args.length == 1 || args.length == 2) {
        boolean isVersion = handleVersion(args);
        if (isVersion) {
          return;
        }
      }

      super.invoke(args);
    } finally {
      // remove zero-sized generatedcode.log file
      try {
        File codeLogFile = new File("generatedcode.log");
        if (codeLogFile.exists() && codeLogFile.isFile() && codeLogFile.length() == 0) {
          codeLogFile.delete();
        }
      } catch (Throwable t) {
        // ignore at this point
      }
    }
  }

  public boolean handleVersion(String[] args) {
    String cmd;
    final ArrayList<String> cmdLine = new ArrayList<>(Arrays.asList(args));
    try {
      Iterator<String> it = cmdLine.iterator();
      while (it.hasNext()) {
        String arg = it.next();
        if (arg.startsWith("-")) {
          checkDashArg(null, arg, it);
        } else {
          break;
        }
      }
    } catch (IllegalArgumentException ex) {
      System.err.println(LocalizedStrings.SystemAdmin_ERROR.toLocalizedString()
          + ": " + getExceptionMessage(ex));
      // fix for bug 28351
      throw new GemFireTerminateError("exiting due to illegal arguments", 1);
    }
    if (cmdLine.size() == 0) {
      if (help) {
        printHelp("gemfire");
      } else {
        System.err.println(LocalizedStrings.SystemAdmin_ERROR_WRONG_NUMBER_OF_COMMAND_LINE_ARGS.toLocalizedString());
        usage();
      }
    }
    cmd = cmdLine.remove(0);
    cmd = checkCmd(cmd);
    try {
      Iterator<String> it = cmdLine.iterator();
      while (it.hasNext()) {
        String arg = it.next();
        if (arg.startsWith("-")) {
          checkDashArg(cmd, arg, it);
        }
      }
    } catch (IllegalArgumentException ex) {
      System.err.println(LocalizedStrings.SystemAdmin_ERROR.toLocalizedString()
          + ": " + getExceptionMessage(ex));
      if (debug) {
        ex.printStackTrace(System.err);
      }
      // fix for bug 28351
      throw new GemFireTerminateError("exiting due to illegal arguments", 1);
    }

    SystemFailure.loadEmergencyClasses();
    if (help) {
      printHelp(cmd);
    }

    if (cmd.equalsIgnoreCase("version")) {
      boolean optionOK = (cmdLine.size() == 0);
      if (cmdLine.size() == 1) {
        String option = cmdLine.get(0);
        if ("CREATE".equals(option) || "FULL".equalsIgnoreCase(option)) {
          optionOK = true;
        }
      }

      if (!optionOK) {
        System.err.println(LocalizedStrings.SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0.
            toLocalizedString(join(cmdLine)));
        usage(cmd);
      }
      if (cmdLine.size() == 1 && ("CREATE".equals(cmdLine.get(0)))) {
        printProductDirectory();
        SnappyDataVersion.createVersionFile();
      } else if (cmdLine.size() == 1 && "FULL".equalsIgnoreCase(cmdLine.get(0))) {
        printProductDirectory();
        SnappyDataVersion.print(System.out, true);
      } else {
        SnappyDataVersion.print(System.out);
      }
      return true;
    }

    return false;
  }
}
