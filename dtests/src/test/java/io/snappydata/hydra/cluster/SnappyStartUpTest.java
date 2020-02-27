/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
import sql.SQLHelper;
import util.TestException;
import util.TestHelper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SnappyStartUpTest extends SnappyTest {

  private static Set<Integer> pids = new LinkedHashSet<>();
  private static Set<String> pidList = new LinkedHashSet<>();

  public static void HydraTask_clusterRestartWithRandomOrderForServerStartUp() {
    Process pr = null;
    ProcessBuilder pb;
    File logFile, log = null, serverKillOutput;
    try {
      HostDescription hd = TestConfig.getInstance().getMasterDescription()
          .getVmDescription().getHostDescription();
      pidList = getServerPidList();
      log = new File(".");
      String server = log.getCanonicalPath() + File.separator + "server.sh";
      logFile = new File(server);
      String serverKillLog = log.getCanonicalPath() + File.separator + "serverKill.log";
      serverKillOutput = new File(serverKillLog);
      FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      List asList = new ArrayList(pidList);
      Collections.shuffle(asList);
      String pidString = String.valueOf(asList.get(0));
      Log.getLogWriter().info("pidString : " + pidString);
      int pid = Integer.parseInt(pidString);
      if (pids.contains(pid)) {
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
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(serverKillOutput));
      pr = pb.start();
      pr.waitFor();
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
    } catch (InterruptedException e) {
      String s = "Exception occurred while waiting for the process execution : " + pr;
      throw new TestException(s, e);
    }
  }

  public static synchronized Set<String> getServerPidList() {
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

  protected static synchronized Set<String> getLeaderPidList() {
    Set<String> pidList = new HashSet<>();
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith("pid") && key.contains("_LeaderLauncher")) {
        String pid = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
        pidList.add(pid);
      }
    }
    Log.getLogWriter().info("Returning leader pid list: " + pidList);
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
        Log.getLogWriter().info("Before shuffle : " + s);
      }
      Collections.shuffle(values);
      for (String s : values) {
        Log.getLogWriter().info("After shuffle : " + s);
      }
    } catch (IOException e) {
      throw new TestException("Error occurred while writing to file: " + srcFile + "\n " + e
          .getMessage());
    }
    doneRandomizing = true;
  }

  public static void HydraTask_ServerHAWithRebalance_clusterRestart() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numVMsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms);
      Long lastCycledTimeForStoreFromBB = (Long) SnappyBB.getBB().getSharedMap().get(LASTCYCLEDTIME);
      snappyTest.cycleVM(numToKill, stopStartVms, "storeVmCycled", lastCycledTimeForStoreFromBB,
          lastCycledTime, "server", true, true, true);
    }
  }

  protected static void serverHAWithRebalance_clusterRestart(String vmDir, String clientName, String vmName) {
    snappyTest.killVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("snappy server stopped successfully...." + vmDir);
    executeOps();
    backUpServerConfigData();
    HydraTask_AddServerNode_Rebalance(clientName, vmName);
    new SnappyStartUpTest().regenerateConfigData(vmDir, "servers", clientName, vmName);
    snappyTest.startVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("snappy server restarted successfully...." + vmDir);
    HydraTask_reWriteServerConfigData();
    backUpServerConfigData();
    HydraTask_stopSnappyCluster();
    Log.getLogWriter().info("snappy cluster stopped successfully...." + vmDir);
    HydraTask_startSnappyCluster();
    Log.getLogWriter().info("snappy cluster restarted successfully...." + vmDir);
  }

  protected static void executeOps() {
    Connection conn = null;
    ResultSet rs = null;
    String query = "create table tab1 (id int, name String, address String) USING  column " +
        "OPTIONS(partition_by 'id')";
    try {
      boolean isSecurityEnabled = (Boolean)SnappyBB.getBB().getSharedMap().get("SECURITY_ENABLED");
      if(isSecurityEnabled)
        conn = getSecuredLocatorConnection("gemfire1","gemfire1");
      else
        conn = getLocatorConnection();
      conn.createStatement().executeUpdate(query);
      Log.getLogWriter().info("query executed successfully: " + query);
      query = "insert into tab1 values(111, 'aaa', 'hello')";
      conn.createStatement().executeUpdate(query);
      query = "insert into tab1 values(222, 'bbb', 'halo')";
      conn.createStatement().executeUpdate(query);
      query = "insert into tab1 values(333, 'aaa', 'hello')";
      conn.createStatement().executeUpdate(query);
      query = "insert into tab1 values(444, 'bbb', 'halo')";
      conn.createStatement().executeUpdate(query);
      query = "insert into tab1 values(555, 'ccc', 'halo')";
      conn.createStatement().executeUpdate(query);
      query = "insert into tab1 values(666, 'ccc', 'halo')";
      conn.createStatement().executeUpdate(query);
      query = "select count(*) from tab1";
      rs = conn.createStatement().executeQuery(query);
      long numRows = 0;
      while (rs.next()) {
        numRows = rs.getLong(1);
        Log.getLogWriter().info("Query : " + query + " executed successfully and query " +
            "result is ::" + numRows);
      }
      if (numRows != 6)
        throw new TestException("Result count mismatch observed in test for table " +
            "tab1 created after stopping all locators. \n Expected Row Count : 6 " + "\n Actual Row" +
            " Count : " + numRows);
      closeConnection(conn);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to release the connection " + TestHelper.getStackTrace(e));
    }
  }

  protected static void dropAndReCreateTable() {
    Connection conn;
    ResultSet rs;
    String query = "DROP TABLE IF EXISTS order_details";
    try {
      boolean isSecurityEnabled = (Boolean)SnappyBB.getBB().getSharedMap().get("SECURITY_ENABLED");
      if(isSecurityEnabled)
        conn = getSecuredLocatorConnection("gemfire1","gemfire1");
      else
        conn = getLocatorConnection();
      conn.createStatement().executeUpdate(query);
      Log.getLogWriter().info("order_details table dropped successfully");
      query = "DROP TABLE IF EXISTS staging_order_details";
      conn.createStatement().executeUpdate(query);
      Log.getLogWriter().info("staging_order_details table dropped successfully");
      query = "CREATE EXTERNAL TABLE staging_order_details" +
          "    USING com.databricks.spark.csv OPTIONS(path '" + SnappyPrms.getDataLocationList()
          .get(0) + "/order-details.csv', header 'true', inferSchema 'true', nullValue 'NULL',  " +
          "maxCharsPerColumn '4096')";
      conn.createStatement().executeUpdate(query);
      Log.getLogWriter().info("staging_order_details table recreated successfully");
      query = "CREATE TABLE order_details USING column OPTIONS(partition_by 'OrderId', buckets" +
          " '13', COLOCATE_WITH 'orders', PERSISTENT 'sync', redundancy '1') AS (SELECT OrderID, " +
          "ProductID, UnitPrice, Quantity, Discount FROM staging_order_details)";
      conn.createStatement().executeUpdate(query);
      Log.getLogWriter().info("staging_order_details table recreated successfully");
      query = "select count(*) from order_details";
      rs = conn.createStatement().executeQuery(query);
      long numRows;
      while (rs.next()) {
        numRows = rs.getLong(1);
        Log.getLogWriter().info("Qyery : " + query + " executed successfully and query " +
            "result is ::" + numRows);
      }
      closeConnection(conn);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to release the connection " + TestHelper.getStackTrace(e));
    }
  }

  public static void HydraTask_AddServerNode_Rebalance(String clientName, String vmName) {
    HydraTask_generateSnappyServerConfig();
    Set<String> newNodeLogDirs = getNewNodeLogDir();
    for (String nodeLogDir : newNodeLogDirs) {
      Log.getLogWriter().info("nodeLogDir is : " + nodeLogDir);
      nodeLogDir = nodeLogDir + " " + " -rebalance ";
      SnappyBB.getBB().getSharedMap().put("serverLogDir" + "_" + RemoteTestModule.getMyVmid() +
          "_" + snappyTest.getMyTid(), nodeLogDir);
      String newNodeLogDir;
      newNodeLogDir = nodeLogDir.substring(nodeLogDir.lastIndexOf("-dir=") + 5);
      newNodeLogDir = newNodeLogDir.substring(0, newNodeLogDir.indexOf(" "));
      Log.getLogWriter().info("New node log dir is : " + newNodeLogDir);
      SnappyBB.getBB().getSharedMap().put("logDir_" + RemoteTestModule.getMyClientName() + "_" +
          RemoteTestModule.getMyVmid(), newNodeLogDir);
      new SnappyStartUpTest().regenerateConfigData(newNodeLogDir, "servers", clientName, vmName);
      Log.getLogWriter().info("nodeLogDir is : " + nodeLogDir);
      new SnappyStartUpTest().startSnappyServer();
    }
  }

  public static void HydraTask_AddServerNode_Rebalance() {
    String clientName = RemoteTestModule.getMyClientName();
    HydraTask_AddServerNode_Rebalance(clientName, "server");
  }

  private static synchronized Set<String> getNewNodeLogDir() {
    Set<String> logDirList = new HashSet<>();
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith("newNodelogDir")) {
        String nodeLogDir = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
        logDirList.add(nodeLogDir);
      }
    }
    Log.getLogWriter().info("Returning new server log directory list: " + logDirList);
    return logDirList;
  }

  /**
   * Generates the configuration data required to start the new snappy Server.
   */
  public static synchronized void HydraTask_generateSnappyServerConfig() {
    SnappyTest server = new SnappyTest(SnappyNode.SERVER);
    server.generateNodeConfig("serverLogDir", true);

  }

  protected static void startSnappyServerWithRebalance(String dirPath, String locators) {
    File log = null;
    ProcessBuilder pb = null;
    try {
      if (useRowStore) {
        Log.getLogWriter().info("Starting server using rebalance and rowstore option...");
        pb = new ProcessBuilder(SnappyShellPath, "server", "start", "-" +
            "", "-dir=" + dirPath,
            "-locators=" + locators, "rowstore");
      } else {
        Log.getLogWriter().info("Starting server using rebalance option...");
        pb = new ProcessBuilder(SnappyShellPath, "server", "start", "-rebalance", "-dir=" + dirPath,
            "-locators=" + locators);
      }
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "snappyServerRebalance.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
    } catch (IOException e) {
      String s = "problem occurred while retriving logFile path " + log;
      throw new TestException(s, e);
    }
  }

  /**
   * Re-write the configuration data required to start the snappy server/s in servers file under
   * conf directory at snappy build location.
   */
  public static void HydraTask_reWriteServerConfigData() {
    String filePath = productConfDirPath + "servers";
    File file = new File(filePath);
    try {
      if (file.exists()) {
        file.delete();
        file.createNewFile();
      }
    } catch (IOException e) {
      String s = "Unable to create file: " + file.getAbsolutePath();
      throw new TestException(s);
    }
    snappyTest.writeConfigData("servers", "serverLogDir");
  }

  /**
   * Add new server configuration data in servers file under testLog directory location which is
   * required to start the snappy server/s in servers including new node later in long running test.
   */
  public static void HydraTask_addNweServerConfigData() {
    File file = null;
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "servers";
      file = new File(dest);
    } catch (IOException e) {
      String s = "Unable to create file: " + file.getAbsolutePath();
      throw new TestException(s);
    }
    snappyTest.writeConfigData("servers", "serverLogDir");
  }

  public static void HydraTask_OpsDuringServerHA_clusterRestart() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numVMsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms);
      Long lastCycledTimeForStoreFromBB = (Long) SnappyBB.getBB().getSharedMap().get(LASTCYCLEDTIME);
      snappyTest.cycleVM(numToKill, stopStartVms, "storeVmCycled", lastCycledTimeForStoreFromBB,
          lastCycledTime, "server", true, true, false);
    }
  }

  protected static void opsDuringServerHA_clusterRestart(String vmDir, String clientName, String vmName) {
    snappyTest.killVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("Snappy server stopped successfully...." + vmDir);
    executeOps();
    dropAndReCreateTable();
    snappyTest.startVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("Snappy server restarted successfully...." + vmDir);
    restoreServerConfigData();
    HydraTask_stopSnappyCluster();
    Log.getLogWriter().info("Snappy cluster stopped successfully...." + vmDir);
    HydraTask_startSnappyCluster();
    Log.getLogWriter().info("Snappy cluster restarted successfully...." + vmDir);
  }

  public static void HydraTask_verifyTableData() {
    Connection conn;
    ResultSet rs;
    String query = null;
    Vector tableNames, numRowsinTables = null;
    long expectedNumRows, actualNumRows = 0;
    try {
      boolean isSecurityEnabled = (Boolean)SnappyBB.getBB().getSharedMap().get("SECURITY_ENABLED");
      if(isSecurityEnabled)
        conn = getSecuredLocatorConnection("gemfire1","gemfire1");
      else
        conn = getLocatorConnection();
      tableNames = SnappyPrms.getTableList();
      numRowsinTables = SnappyPrms.getNumRowsList();
      if (tableNames.isEmpty() || numRowsinTables.isEmpty()) {
        throw new TestException("Either list of tables or number of rows against tableNames " +
            "required for validation is not specified");
      }
      if (tableNames.size() != numRowsinTables.size()) {
        Log.getLogWriter().info("Mismatch observed in expected number of rows list and tables " +
            "list. Please verify the configuration again.");
      }
      for (int i = 0; i < tableNames.size(); i++) {
        String tableName = (String) tableNames.elementAt(i);
        query = "select count(*) from " + tableName;
        rs = conn.createStatement().executeQuery(query);
        expectedNumRows = Long.parseLong((String) numRowsinTables.elementAt(i));
        while (rs.next()) {
          actualNumRows = rs.getLong(1);
          Log.getLogWriter().info("Qyery : " + query + " executed successfully and query " +
              "result is ::" + actualNumRows);
        }
        if (actualNumRows != expectedNumRows) {
          throw new TestException("Mismatch observed. Expected " + expectedNumRows +
              "number of rows, " + "but observed actual Number of rows " + actualNumRows + " in " +
              "table " + tableName);
        }
        long numRowsPrimaryBeforeRestart = (long) SnappyBB.getBB().getSharedMap().get
            ("numRowsPrimaryBeforeRestart");
        long numRowsSecondaryBeforeRestart = (long) SnappyBB.getBB().getSharedMap().get
            ("numRowsSecondaryBeforeRestart");

        long numRowsPrimaryAfterRestart = (long) SnappyBB.getBB().getSharedMap().get
            ("numRowsPrimaryAfterRestart");
        long numRowsSecondaryAfterRestart = (long) SnappyBB.getBB().getSharedMap().get
            ("numRowsSecondaryAfterRestart");

        if (numRowsPrimaryBeforeRestart != numRowsPrimaryAfterRestart) {
          throw new TestException("Mismatch observed. Expected " + numRowsPrimaryBeforeRestart +
              "number of rows in primary buckets, " + "but observed actual Number of rows in " +
              "primary buckets" +
              " " + numRowsPrimaryAfterRestart + " for " +
              "table " + tableName);
        } else {
          Log.getLogWriter().info("Got expected number of rows : " +
              numRowsPrimaryAfterRestart + " in primary buckets before and " +
              "after cluster restart");
        }
        if (numRowsSecondaryBeforeRestart != numRowsSecondaryAfterRestart) {
          throw new TestException("Mismatch observed. Expected " + numRowsSecondaryBeforeRestart +
              "number of rows in secondary buckets, " + "but observed actual Number of rows in " +
              "secondary buckets" +
              " " + numRowsSecondaryAfterRestart + " for " +
              "table " + tableName);
        } else {
          Log.getLogWriter().info("Got expected number of rows : " +
              numRowsSecondaryAfterRestart + " in secondary buckets before and " +
              "after cluster restart");
        }
      }
      Vector indexNames = SnappyPrms.getIndexList();
      for (int j = 0; j < indexNames.size(); j++) {
        String indexName = (String) indexNames.elementAt(j);
        long numRowsIndexBeforeRestart = (long) SnappyBB.getBB().getSharedMap().get
            ("numRows_" + indexName + "_BeforeRestart");
        long numRowsIndexAfterRestart = (long) SnappyBB.getBB().getSharedMap().get
            ("numRows_" + indexName + "_AfterRestart");
        if (numRowsIndexBeforeRestart != numRowsIndexAfterRestart) {
          throw new TestException("Mismatch observed. Expected " + numRowsIndexBeforeRestart +
              "number of rows in index: " + indexName + ", " + "but observed " +
              numRowsIndexAfterRestart + " number of rows.");
        } else {
          Log.getLogWriter().info("Got expected number of rows : " +
              numRowsIndexAfterRestart + " in index: " + indexName + " before and " +
              "after cluster restart");
        }
      }
      closeConnection(conn);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Got Exception: " + e.getMessage() + "\n" + TestHelper.getStackTrace
          (e));
    }
  }

  public static void HydraTask_executeDiagnosticQueriesBeforeRecovery() {
    executeDiagnosticQueries("Before");
  }

  public static void executeDiagnosticQueries(String queryExecutionTime) {
    Connection conn;
    ResultSet rs;
    String query = null;
    Vector tableNames = SnappyPrms.getTableList(), indexNames = SnappyPrms.getIndexList();
    long numRowsPrimary = 0, numRowsPrimarySecondary = 0, numRowsSecondary = 0, numRowsIndex
        = 0;
    try {
      conn = getLocatorConnectionUsingProps();
      if (tableNames.isEmpty()) {
        throw new TestException("List of tables against tableNames " +
            "required for diagnostic query execution is not specified");
      }
      if (indexNames.isEmpty()) {
        throw new TestException("List of indexes against tableNames " +
            "required for diagnostic query execution is not specified");
      }
      for (int i = 0; i < tableNames.size(); i++) {
        String tableName = (String) tableNames.elementAt(i);
        query = "select count(*), dsid() from sys.members m --GEMFIREXD-PROPERTIES withSecondaries=false \n , " + tableName + "  where dsid() = m.id";
        rs = conn.createStatement().executeQuery(query);
        while (rs.next()) {
          numRowsPrimary = rs.getLong(1);
          Log.getLogWriter().info("Qyery : " + query + " executed successfully and found " +
              numRowsPrimary + " rows in primary buckets " +
              queryExecutionTime + "  cluster restart.");
        }
        query = "select count(*), dsid() from sys.members m --GEMFIREXD-PROPERTIES withSecondaries=true \n , " + tableName + "  where dsid() = m.id";
        rs = conn.createStatement().executeQuery(query);
        while (rs.next()) {
          numRowsPrimarySecondary = rs.getLong(1);
          Log.getLogWriter().info("Qyery : " + query + " executed successfully and and found " +
              numRowsPrimarySecondary + " rows in primary and secondary buckets " +
              queryExecutionTime + " cluster " +
              "restart.");
        }
        numRowsSecondary = numRowsPrimarySecondary - numRowsPrimary;
        SnappyBB.getBB().getSharedMap().put("numRowsPrimary" +
            queryExecutionTime + "Restart", numRowsPrimary);
        SnappyBB.getBB().getSharedMap().put("numRowsSecondary" + queryExecutionTime + "Restart", numRowsSecondary);

        for (int j = 0; j < indexNames.size(); j++) {
          String indexName = (String) indexNames.elementAt(j);
          String tableNameString = tableName.substring(tableName.indexOf(".") + 1);
          if (indexName.contains(tableNameString)) {
            query = "select count(*), dsid() from sys.members m , " + tableName + " " +
                "--GEMFIREXD-PROPERTIES index=" + indexName + "  \n where dsid() = m.id ";
            rs = conn.createStatement().executeQuery(query);
            while (rs.next()) {
              numRowsIndex = rs.getLong(1);
              Log.getLogWriter().info("Qyery : " + query + " executed successfully and and found " +
                  numRowsIndex + " rows in index:  " + indexName + " on table: " + tableName +
                  queryExecutionTime + " cluster restart.");
            }
            SnappyBB.getBB().getSharedMap().put("numRows_" + indexName + "_" +
                queryExecutionTime + "Restart", numRowsIndex);
          }
        }
      }
      closeConnection(conn);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to release the connection " + TestHelper.getStackTrace(e));
    }
  }


  public static void HydraTask_executeDiagnosticQueriesAfterRecovery() {
    executeDiagnosticQueries("After");
  }
}
