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

import hydra.Log;
import hydra.TestConfig;
import sql.SQLHelper;
import util.TestException;
import util.TestHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;


public class SnappyLocatorHATest extends SnappyTest {

  /**
   * Concurrently stops a List of snappy locator VMs, execute the ddl op and then restarts
   * them.  Waits for the restart to complete before returning.
   */
  public static synchronized void HydraTask_ddlOpDuringLocatorHA() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numVMsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms);
      Long lastCycledTimeForLocatorFromBB = (Long) SnappyBB.getBB().getSharedMap().get
          (LASTCYCLEDTIMEFORLOCATOR);
      snappyTest.cycleVM(numToKill, stopStartVms, "locatorVmCycled", lastCycledTimeForLocatorFromBB,
          lastCycledTime, "locator", true, false, false);
    }
  }

  protected static void ddlOpDuringLocatorHA(String vmDir, String clientName, String vmName) {
    snappyTest.killVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("snappy locator stopped successfully...." + vmDir);
    executeOps();
    snappyTest.startVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("snappy locator restarted successfully...." + vmDir);
  }

  protected static void executeOps() {
    Connection conn = null;
    ResultSet rs = null;
    String query = "create table tab1 (id int, name String, address String) USING  column " +
        "OPTIONS(partition_by 'id')";
    try {
      conn = getServerConnection();
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
        Log.getLogWriter().info("Qyery : " + query + " executed successfully and query " +
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

  /**
   * Concurrently stops a List of snappy locator VMs, perform ddl op and then stops and restarts
   * the snappy cluster.  Waits for the restart to complete before returning.
   */
  public static void HydraTask_ddlOpAfterLocatorStop_ClusterRestart() {
    if (cycleVms) {
      int numToKill = TestConfig.tab().intAt(SnappyPrms.numVMsToStop, 1);
      int stopStartVms = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms);
      Long lastCycledTimeForLocatorFromBB = (Long) SnappyBB.getBB().getSharedMap().get
          (LASTCYCLEDTIMEFORLOCATOR);
      snappyTest.cycleVM(numToKill, stopStartVms, "locatorVmCycled", lastCycledTimeForLocatorFromBB,
          lastCycledTime, "locator", true, true, false);
    }
  }

  protected static void ddlOpAfterLocatorStop_ClusterRestart(String vmDir, String clientName,
                                                             String vmName) {
    snappyTest.killVM(vmDir, clientName, vmName);
    Log.getLogWriter().info("snappy locator stopped successfully...." + vmDir);
    executeOps();
    HydraTask_stopSnappyCluster();
    Log.getLogWriter().info("snappy cluster stopped successfully...." + vmDir);
    HydraTask_startSnappyCluster();
    Log.getLogWriter().info("snappy cluster restarted successfully...." + vmDir);
  }

  public static void HydraTask_ddlOpAfterAllLocatorStop_ClusterRestart() {
    HydraTask_stopSnappyLocator();
    Log.getLogWriter().info("snappy locators stopped successfully....");
    executeOps();
    HydraTask_stopSnappyCluster();
    Log.getLogWriter().info("snappy cluster stopped successfully....");
    HydraTask_startSnappyCluster();
    Log.getLogWriter().info("snappy cluster restarted successfully....");
  }

  public static void HydraTask_validateTableDataOnClusterRestart() {
    try {
      Connection conn = null;
      boolean isSecurityEnabled = (Boolean)SnappyBB.getBB().getSharedMap().get("SECURITY_ENABLED");
      if(isSecurityEnabled)
        conn = getSecuredLocatorConnection("gemfire1","gemfire1");
      else
        conn = getLocatorConnection();
      String query = "select count(*) from tab1";
      ResultSet rs = conn.createStatement().executeQuery(query);
      long numRows = 0;
      while (rs.next()) {
        numRows = rs.getLong(1);
        Log.getLogWriter().info("Qyery : " + query + " executed successfully and query " +
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
}
