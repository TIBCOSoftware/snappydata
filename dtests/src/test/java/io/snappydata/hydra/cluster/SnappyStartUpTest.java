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
package io.snappydata.hydra.cluster;

import hydra.*;
import util.TestException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.*;

public class SnappyStartUpTest extends SnappyTest {

  private static Set<Integer> pids = new LinkedHashSet<Integer>();
  private static Set<String> pidList = new LinkedHashSet<String>();

  public static void HydraTask_clusterRestartWithRandomOrderForServerStartUp() {
    Process pr = null;
    ProcessBuilder pb;
    File logFile = null, log = null, serverKillOuput;
    try {
      HostDescription hd = TestConfig.getInstance().getMasterDescription()
          .getVmDescription().getHostDescription();
      pidList = getServerPidList();
      log = new File(".");
//      if(logFile.exists()) logFile.renameTo("server.sh");
        String server = log.getCanonicalPath() + File.separator + "server.sh";
        logFile = new File(server);
      String serverKillLog = log.getCanonicalPath() + File.separator + "serverKill.log";
      serverKillOuput = new File(serverKillLog);
      FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      List asList = new ArrayList(pidList);
      Collections.shuffle(asList);
      String pidString = String.valueOf(asList.get(0));
      Log.getLogWriter().info("pidString : " + pidString);
      int pid = Integer.parseInt(pidString);
      if(pids.contains(pid)) {
        pidList.remove(pidString);
        asList = new ArrayList(pidList);
        Collections.shuffle(asList);
        pidString = String.valueOf(asList.get(0));
        Log.getLogWriter().info("pidString : " + pidString);
        pid = Integer.parseInt(pidString);
      }
      pids.add(pid);
      Log.getLogWriter().info("Server Pid chosen for abrupt kill : " + pidString);
      String pidHost = snappyTest.getPidHost(Integer.toString(pid));
      if (pidHost.equalsIgnoreCase("localhost")) {
        bw.write("/bin/kill -KILL " + pid);
      } else {
        bw.write("ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " +
            pidHost + " /bin/kill -KILL " + pid);
      }
      bw.newLine();
      try {
        RemoteTestModule.Master.removePID(hd, pid);
      } catch (RemoteException e) {
        String s = "Failed to remove PID from nukerun script: " + pid;
        throw new HydraRuntimeException(s, e);
      }
      bw.close();
      fw.close();
      logFile.setExecutable(true);
      pb = new ProcessBuilder(server);
      pb.redirectErrorStream(true);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(serverKillOuput));
      pr = pb.start();
      pr.waitFor();
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new TestException(s, e);
    }
  }

  private static synchronized Set<String> getServerPidList() {
    Set<String> pidList = new HashSet<>();
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith("pid") && key.contains("_ServerLauncher")) {
        String pid = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
        pidList.add(pid);
      }
    }
    Log.getLogWriter().info("Returning server pid list: " + pidList);
    return pidList;
  }

  /**
   * Mandatory to use this method in case of clusterRestartWithRandomOrderForServerStartUp test.
   * As per current implementation, for starting the server snappy-servers.sh script is used, which starts
   * the servers based on the data in servers conf file.
   * In HA test, the framework deletes the old servers file and creates the new one with the config data specific
   * to server which is getting recycled.
   * So, we need to backup the original servers conf file and then shuffle the order of server
   * configs required to start the servers in random order while restarting the cluster.
   **/
  public static synchronized void randomizeServerConfigData() {
    randomizeConfigData("servers");
  }

  protected static void randomizeConfigData(String fileName) {
    if (doneRandomizing) return;
    File srcDir = new File(".");
    File srcFile = null;
    try {
      String srcFilePath = srcDir.getCanonicalPath() + File.separator + fileName;
      srcFile = new File(srcFilePath);
      List<String> values = Files.readAllLines(Paths.get(srcFile.getPath()));
      for (String s : values) {
        Log.getLogWriter().info("SS - before shuffle : " + s);
      }
      Collections.shuffle(values);
      for (String s : values) {
        Log.getLogWriter().info("SS - after shuffle : " + s);
      }
    } catch (IOException e) {
      throw new TestException("Error occurred while writing to file: " + srcFile + "\n " + e
          .getMessage());
    }
    doneRandomizing = true;
  }

}
