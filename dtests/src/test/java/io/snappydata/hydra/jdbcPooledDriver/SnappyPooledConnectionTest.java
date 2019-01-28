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

package io.snappydata.hydra.jdbcPooledDriver;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.security.SnappySecurityPrms;
import util.TestException;


public class SnappyPooledConnectionTest extends SnappyTest {
  public static SnappyPooledConnectionTest snappyPooledConnectionTest;

   public static void HydraTask_runQuery() {
     if (snappyPooledConnectionTest == null) {
       snappyPooledConnectionTest = new SnappyPooledConnectionTest();
     }
     snappyPooledConnectionTest.executeQuery();
   }

   public Connection getPooledConnection() throws SQLException {
     List<String> endpoints = validateLocatorEndpointData();
     Connection conn = null;
     String Driver =null;
     String url = null;
     boolean isPooledUrl = SnappyPooledConnectionPrms.getIsPooledConnection();
     boolean isSecurity = false;
     Properties props = new Properties();
     if(isPooledUrl)
     {
       Driver = "io.snappydata.jdbc.ClientPoolDriver";
       url = "jdbc:snappydata:pool://" + endpoints.get(0) + "/";
     }
     else {
       Driver = "io.snappydata.jdbc.ClientDriver";
       url = "jdbc:snappydata://" + endpoints.get(0) + "/";
     }
     if(isSecurity)
     {
       props.setProperty("pool-user", "");
       props.setProperty("pool-password", "");
     }
     conn = getConnection(url, Driver, props);
     return conn;
   }

  private static Connection getConnection(String protocol, String driver, Properties props)
      throws
      SQLException {
    Log.getLogWriter().info("Creating connection using " + driver + " with " + protocol +
        " and user specified properties list: = " + props.toString());
    loadDriver(driver);
    Connection conn = DriverManager.getConnection(protocol, props);
    return conn;
  }

   public void executeQuery(){
     try {
       Connection conn = null;
       conn = getPooledConnection();
       String q1="SELECT * FROM employees limit 5;";
       ResultSet rs = conn.createStatement().executeQuery(q1);
       while(rs.next())
          Log.getLogWriter().info("The EmployeeID is " + rs.getString("EmployeeID"));
     }
     catch(SQLException ex){
       Log.getLogWriter().info("Caught exception in executeQuery " + ex.getMessage() );
     }
   }

  /**
   * Starts Spark Cluster with the specified number of workers.
   */
  public static synchronized void HydraTask_startSparkCluster() {
    File log = null;
    try {
    //  int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.sparkClusterStarted);
   //   if (num == 1) {
        // modifyJobServerConfig();
        String sparkScriptPath = SnappySecurityPrms.getDataLocation();
        String cmd = sparkScriptPath + "/sbin/start-all.sh";
        ProcessBuilder pb = new ProcessBuilder(cmd);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "sparkSystem.log";
        File logFile = new File(dest);
        snappyTest.executeProcess(pb, logFile);
     // }
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

  /**
   * Stops Spark Cluster.
   */
  public static synchronized void HydraTask_stopSparkCluster() {
    File log = null;
    try {
      initSnappyArtifacts();
      String sparkScriptPath = SnappySecurityPrms.getDataLocation();
      String cmd = sparkScriptPath + "/sbin/stop-all.sh";
      ProcessBuilder pb = new ProcessBuilder(cmd);
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "sparkSystem.log";
      File logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
    //  SnappyBB.getBB().getSharedCounters().zero(SnappyBB.sparkClusterStarted);
    } catch (IOException e) {
      String s = "problem occurred while retriving destination logFile path " + log;
      throw new TestException(s, e);
    }
  }

}

