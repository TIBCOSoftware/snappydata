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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;


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
     String Driver = "io.snappydata.jdbc.ClientPoolDriver";
     Properties props = new Properties();
     props.setProperty("pool-user", "");
     props.setProperty("pool-password", "");
     String url = "jdbc:snappydata:pool://" + endpoints.get(0) + "/";
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
       conn.createStatement().execute("");
     }
     catch(SQLException ex){
       Log.getLogWriter().info("Caught exception in executeQuery " + ex.getMessage() );
     }
   }

}

