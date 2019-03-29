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

package io.snappydata.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;

import com.gemstone.gemfire.internal.cache.Status;
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.gemstone.gemfire.internal.shared.LauncherBase;
import com.sun.jna.Platform;

class QuickLauncher extends LauncherBase {

  private static final String DIR_OPT = "-dir=";
  private static final String CLASSPATH_OPT = "-classpath=";
  private static final String HEAP_SIZE_OPT = "-heap-size=";
  private static final String WAIT_FOR_SYNC_OPT = "-sync=";
  private static final String PASSWORD_OPT = "-password";

  private final String launcherClass;
  private final boolean isLocator;
  private Path workingDir;

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw illegalArgument("QuickLauncher: expected at least two args", args);
    }

    String nodeType = args[0];
    String action = args[1];
    QuickLauncher launch;
    if (nodeType.equalsIgnoreCase("server")) {
      launch = new QuickLauncher("SnappyData Server", "snappyserver",
          "io.snappydata.tools.ServerLauncher", false);
    } else if (nodeType.equalsIgnoreCase("leader")) {
      launch = new QuickLauncher("SnappyData Leader", "snappyleader",
          "io.snappydata.tools.LeaderLauncher", false);
      // always wait till everything is done for leads
      launch.waitForData = true;
    } else if (nodeType.equalsIgnoreCase("locator")) {
      launch = new QuickLauncher("SnappyData Locator", "snappylocator",
          "io.snappydata.tools.LocatorLauncher", true);
    } else {
      throw illegalArgument("QuickLauncher: unknown node type '" + nodeType + "'", args);
    }

    if ("start".equalsIgnoreCase(action)) {
      System.exit(launch.start(args));
    } else if ("stop".equalsIgnoreCase(action)) {
      System.exit(launch.stop(args));
    } else if ("status".equalsIgnoreCase(action)) {
      launch.status(args);
    } else {
      throw illegalArgument("QuickLauncher: unsupported action '" + action + "'", args);
    }
  }

  private QuickLauncher(String displayName, String baseName,
      String launcherClass, boolean locator) {
    super(displayName, baseName);
    this.launcherClass = launcherClass;
    this.isLocator = locator;
    // don't wait for diskstore sync by default and go into WAITING state
    this.waitForData = false;
  }

  @Override
  protected Path getWorkingDirPath() {
    return this.workingDir;
  }

  @Override
  protected long getDefaultHeapSizeMB(boolean hostData) {
    return this.isLocator ? 1024L : super.getDefaultHeapSizeMB(hostData);
  }

  @Override
  protected long getDefaultSmallHeapSizeMB(boolean hostData) {
    return this.isLocator ? 512L : super.getDefaultSmallHeapSizeMB(hostData);
  }

  private static IllegalArgumentException illegalArgument(String message, String[] args) {
    return new IllegalArgumentException(message + " (args=" +
        java.util.Arrays.toString(args) + ')');
  }

  /**
   * Configures and spawns a VM that hosts a cache server. The standard output
   * and error are redirected to file prefixed with "start_".
   */
  private int start(final String[] args) throws IOException, InterruptedException {
    final ArrayList<String> commandLine = new ArrayList<>(16);
    final HashMap<String, String> env = new HashMap<>(2);
    final String snappyHome = System.getenv("SNAPPY_HOME");

    if (snappyHome == null || snappyHome.isEmpty()) {
      throw new IllegalArgumentException("SNAPPY_HOME not set");
    }

    // add java path to command-line first
    commandLine.add(System.getProperty("java.home") + "/bin/java");
    commandLine.add("-server");

    try {
      // https://github.com/airlift/jvmkill is added to libgemfirexd.so
      // adding agentpath helps kill jvm in case of OOM. kill -9 is not
      // used as it fails in certain cases
      if (Platform.isLinux()) {
        if (Platform.is64Bit()) {
          String agentPath = snappyHome + "/jars/libgemfirexd64.so";
          System.load(agentPath);
          commandLine.add("-agentpath:" + agentPath);
        } else {
          String agentPath = snappyHome + "/jars/libgemfirexd.so";
          System.load(agentPath);
          commandLine.add("-agentpath:" + agentPath);
        }
      } else if (Platform.isMac()) {
        if (Platform.is64Bit()) {
          String agentPath = snappyHome + "/jars/libgemfirexd64.dylib";
          System.load(agentPath);
          commandLine.add("-agentpath:" + agentPath);
        } else {
          String agentPath = snappyHome + "/jars/libgemfirexd.dylib";
          System.load(agentPath);
          commandLine.add("-agentpath:" + agentPath);
        }
      }
    } catch (UnsatisfiedLinkError | SecurityException e) {
      System.out.println("WARNING: agent not loaded due to " + e +
          ". Service might not be killed on OutOfMemory. Build jvmkill.c on your platform " +
          "using build.sh script from source on your platform and replace the library " +
          "in product jars directory to enable the agent.");
    }
    // get the startup options and command-line arguments (JVM arguments etc)
    HashMap<String, Object> options = getStartOptions(args, snappyHome, commandLine, env);

    // Complain if a cache server is already running in the specified working directory.
    // See bug 32574.
    String msg = verifyAndClearStatus();
    if (msg != null) {
      System.err.println(msg);
      return 1;
    }

    // add the main class and method
    commandLine.add(this.launcherClass);
    commandLine.add("server");
    // add default log-file option if not present
    String logFile = (String)options.get(LOG_FILE);
    if (logFile == null || logFile.isEmpty()) {
      // check for log4j settings
      Properties confProps = ClientSharedUtils.getLog4jConfProperties(snappyHome);
      if (confProps != null) {
        // read directly avoiding log4j API usage
        String rootCategory = confProps.getProperty("log4j.rootCategory");
        int commaIndex;
        if (rootCategory != null && (commaIndex = rootCategory.indexOf(',')) != -1) {
          String categoryName = rootCategory.substring(commaIndex + 1).trim();
          // check for file name property
          logFile = confProps.getProperty("log4j.appender." + categoryName + ".file");
          if (logFile == null || logFile.isEmpty()) {
            // perhaps it is the console appender
            String appenderType = confProps.getProperty("log4j.appender." + categoryName);
            if (appenderType != null && appenderType.contains("ConsoleAppender")) {
              logFile = this.startLogFileName;
            }
          }
        }
      }
    } else {
      logFile = logFile.substring(LOG_FILE.length() + 2);
    }
    if (logFile == null || logFile.isEmpty()) {
      logFile = this.defaultLogFileName;
    }
    options.put(LOG_FILE, "-" + LOG_FILE + '=' + logFile);

    // add the command options
    for (Object option : options.values()) {
      commandLine.add((String)option);
    }

    // finally launch the main process
    final Path startLogFile = this.workingDir.resolve(startLogFileName);
    final Path pidFile = this.workingDir.resolve(pidFileName);
    Files.deleteIfExists(startLogFile);
    Files.deleteIfExists(pidFile);

    // add command-line to ENV2
    StringBuilder fullCommandLine = new StringBuilder(1024);
    fullCommandLine.append(ENV_MARKER).append('[');
    Iterator<String> cmds = commandLine.iterator();
    while (cmds.hasNext()) {
      String cmd = cmds.next();
      if (cmd.equals("-cp")) {
        // skip classpath which is already output separately
        cmds.next();
      } else {
        if (cmd.indexOf(' ') != -1) {
          // quote the argument
          fullCommandLine.append('"').append(cmd).append('"');
        } else {
          fullCommandLine.append(cmd);
        }
        fullCommandLine.append(' ');
      }
    }
    fullCommandLine.setCharAt(fullCommandLine.length() - 1, ']');
    env.put(ENV2, fullCommandLine.toString());

    ProcessBuilder process = new ProcessBuilder(commandLine);
    process.environment().putAll(env);
    process.directory(this.workingDir.toFile());
    process.redirectErrorStream(true);
    process.redirectOutput(startLogFile.toFile());
    process.start();

    // change to absolute path for printing to screen
    if (!Paths.get(logFile).isAbsolute()) {
      logFile = this.workingDir.resolve(logFile).toString();
    }
    return waitForRunning(logFile);
  }

  /**
   * Stops a node (which is running in a different VM) by setting its status to
   * {@link Status#SHUTDOWN_PENDING}. Waits for the node to actually shut down
   * and returns the exit status (0 is for success else failure).
   */
  private int stop(final String[] args) throws IOException {
    setWorkingDir(args);
    final Path statusFile = getStatusPath();
    int exitStatus = 1;

    // determine the current state of the node
    readStatus(false, statusFile);
    if (this.status != null) {
      // upon reading the status file, request the Cache Server to shutdown
      // if it has not already...
      if (this.status.state != Status.SHUTDOWN) {
        // copy server PID and not use own PID; see bug #39707
        this.status = Status.create(this.baseName, Status.SHUTDOWN_PENDING,
            this.status.pid, statusFile);
        this.status.write();
      }

      // poll the Cache Server for a response to our shutdown request
      // (passes through if the Cache Server has already shutdown)
      pollCacheServerForShutdown(statusFile);

      // after polling, determine the status of the Cache Server one last time
      // and determine how to exit...
      if (this.status.state == Status.SHUTDOWN) {
        System.out.println(MessageFormat.format(LAUNCHER_STOPPED,
            this.baseName, getHostNameAndDir()));
        Status.delete(statusFile);
        exitStatus = 0;
      } else {
        System.out.println(MessageFormat.format(LAUNCHER_TIMEOUT_WAITING_FOR_SHUTDOWN,
            this.baseName, this.hostName, this.status));
      }
    } else if (Files.exists(statusFile)) {
      throw new IllegalStateException(MessageFormat.format(
          LAUNCHER_NO_AVAILABLE_STATUS, this.statusName));
    } else {
      System.out.println(MessageFormat.format(LAUNCHER_NO_STATUS_FILE,
          this.workingDir, this.hostName));
    }
    return exitStatus;
  }

  /**
   * Prints the status of the node running in the configured working directory.
   */
  private void status(final String[] args) throws FileNotFoundException {
    setWorkingDir(args);
    final Path statusFile = this.workingDir.resolve(this.statusName);
    readStatus(true, statusFile);
    if (args.length > 2 && args[2].equalsIgnoreCase("verbose")) {
      System.out.println(this.status);
    } else {
      System.out.println(this.status.shortStatus());
    }
  }

  private String getHostNameAndDir() {
    return workingDir != null ? hostName + '(' + workingDir.getFileName() + ')'
        : this.hostName;
  }

  /**
   * Get the name, value pairs of start options provided on command-line.
   * Value is the entire argument including the name and starting hyphen.
   */
  private HashMap<String, Object> getStartOptions(String[] args, String snappyHome,
      ArrayList<String> commandLine, HashMap<String, String> env)
      throws FileNotFoundException {
    StringBuilder classPath = new StringBuilder();
    // add the conf directory first
    classPath.append(snappyHome).append("/conf").append(java.io.File.pathSeparator);
    // then the default product jars directory
    classPath.append(snappyHome).append("/jars/*");

    final int numArgs = args.length;
    final LinkedHashMap<String, Object> options = new LinkedHashMap<>(numArgs);
    final ArrayList<String> vmArgs = new ArrayList<>(2);
    boolean hostData = !this.isLocator;

    // skip first two arguments for node type and action
    for (int i = 2; i < numArgs; i++) {
      String arg = args[i];
      // process special arguments separately
      if (arg.startsWith(DIR_OPT)) {
        processDirOption(arg.substring(DIR_OPT.length()));
      } else if (arg.startsWith(CLASSPATH_OPT)) {
        classPath.append(java.io.File.pathSeparator)
            .append(arg.substring(CLASSPATH_OPT.length()));
      } else if (arg.startsWith(HEAP_SIZE_OPT)) {
        processHeapSize(arg.substring(HEAP_SIZE_OPT.length()), vmArgs);
      } else if (arg.startsWith("-J")) {
        processVMArg(arg.substring(2), vmArgs);
      } else if (arg.startsWith("-D") || arg.startsWith("-XX:")) {
        processVMArg(arg, vmArgs);
      } else if (arg.startsWith(PASSWORD_OPT)) {
        String pwd;
        if (arg.length() > PASSWORD_OPT.length()) {
          // next character must be '='
          if (arg.charAt(PASSWORD_OPT.length()) != '=') {
            throw new IllegalArgumentException(
                "Unexpected option (should be -password or -password=): " + arg);
          }
          pwd = arg.substring(PASSWORD_OPT.length() + 1);
        } else {
          pwd = LauncherBase.readPassword("Password: ");
        }
        if (pwd != null) {
          // encryption should be done throughout from source and kept
          // encrypted throughout in the system (SNAP-1657)
          env.put(ENV1, ENV_MARKER + pwd);
        }
      } else if (arg.startsWith(WAIT_FOR_SYNC_OPT)) {
        processWaitForSync(arg.substring(WAIT_FOR_SYNC_OPT.length()));
      } else if (arg.length() > 0 && arg.charAt(0) == '-') {
        // put rest of the arguments as is
        int eqIndex = arg.indexOf('=');
        String key = eqIndex != -1 ? arg.substring(1, eqIndex) : arg.substring(1);
        options.put(key, arg);
        if (key.equals(HOST_DATA) && eqIndex != -1) {
          hostData = !"false".equalsIgnoreCase(arg.substring(eqIndex + 1));
        }
      } else {
        throw new IllegalArgumentException("Unexpected command-line option: " + arg);
      }
    }
    // set the classpath
    commandLine.add("-cp");
    commandLine.add(classPath.toString());
    // set default JVM args
    commandLine.add("-Djava.awt.headless=true");
    // configure commons-logging to use Log4J logging
    commandLine.add("-Dorg.apache.commons.logging.Log=" +
        "org.apache.commons.logging.impl.Log4JLogger");
    setDefaultVMArgs(options, hostData, commandLine);
    // For lead node set the member-weight to be 17
    if (launcherClass.equals("io.snappydata.tools.LeaderLauncher")) {
      vmArgs.add("-Dgemfire.member-weight=17");
    }
    // add the provided JVM args after the defaults
    if (!vmArgs.isEmpty()) {
      commandLine.addAll(vmArgs);
    }

    return options;
  }

  /**
   * Extracts working directory configuration from command-line arguments
   * for stop/status/wait/verbose commands.
   */
  private void setWorkingDir(String[] args) throws FileNotFoundException {
    String workDir = null;
    // skip first node type argument (locator/server/lead)
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith(DIR_OPT)) {
        workDir = arg.substring(DIR_OPT.length());
      } else if (!arg.equalsIgnoreCase("stop") &&
          !arg.equalsIgnoreCase("status") &&
          !arg.equalsIgnoreCase("wait") &&
          !arg.equalsIgnoreCase("verbose")) {
        throw new IllegalArgumentException(
            MessageFormat.format(LAUNCHER_UNKNOWN_ARGUMENT, arg));
      }
    }
    processDirOption(workDir);
  }

  private void processDirOption(String dir) throws FileNotFoundException {
    if (dir != null) {
      final Path workingDirectory = Paths.get(dir);
      if (Files.exists(workingDirectory)) {
        this.workingDir = workingDirectory.toAbsolutePath(); // see bug 32548
      } else {
        throw new FileNotFoundException(
            MessageFormat.format(LAUNCHER_WORKING_DIRECTORY_DOES_NOT_EXIST, dir));
      }
    } else {
      // default to current working directory
      this.workingDir = Paths.get(".");
    }
  }

  /**
   * Returns the <code>Status</code> of the node. An empty status is returned
   * if status file is missing and <code>emptyForMissing</code> argument is true
   * else null is returned.
   */
  private void readStatus(boolean emptyForMissing, final Path statusFile) {
    this.status = null;
    if (Files.exists(statusFile)) {
      this.status = Status.spinRead(baseName, statusFile);
    }
    if (this.status == null && emptyForMissing) {
      this.status = Status.create(baseName, Status.SHUTDOWN, 0, statusFile);
    }
  }
}
