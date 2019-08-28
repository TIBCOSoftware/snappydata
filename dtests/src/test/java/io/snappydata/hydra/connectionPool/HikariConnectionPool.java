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

package io.snappydata.hydra.connectionPool;

import java.sql.Connection;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

public class HikariConnectionPool {

  public static HikariConnectionPool connPoolInstance = null;
  public static HikariDataSource datasource;

  private HikariConnectionPool() {
    Log.getLogWriter().info(" Creating instance of HikariConnectionPool");
    String url = SnappyConnectionPoolPrms.getUrl() + SnappyTest.validateLocatorEndpointData().get
        (0);
    Properties connProperties = new Properties();
    //user connection properties
    String[] strArr = SnappyConnectionPoolPrms.getConnProperties();
    for(int i = 0; i < strArr.length; i++) {
      String prop[] = strArr[i].split("=");
      connProperties.setProperty(prop[0],prop[1]);
    }
    HikariConfig jdbcConfig = new HikariConfig(connProperties);
    jdbcConfig.setPoolName(SnappyConnectionPoolPrms.getPoolName());
    jdbcConfig.setMaximumPoolSize(SnappyConnectionPoolPrms.getInitialSize());
    jdbcConfig.setJdbcUrl(url);
    jdbcConfig.setDriverClassName(SnappyConnectionPoolPrms.getDriver());
    jdbcConfig.setUsername(SnappyConnectionPoolPrms.getUsername());
    jdbcConfig.setPassword(SnappyConnectionPoolPrms.getPassword());
    datasource = new HikariDataSource(jdbcConfig);
  }

  public static HikariConnectionPool getInstance(){
    if (connPoolInstance == null) 
      connPoolInstance = new HikariConnectionPool();
    return connPoolInstance;
  }

  public static synchronized Connection getConnection() {
    HikariConnectionPool connPool = HikariConnectionPool.getInstance();
    Connection conn = null;
    try {
      conn = connPool.datasource.getConnection();
    }
    catch(Exception e) {
      Log.getLogWriter().info("Got exception while getting connection using hikari connection " +
          "pool");
      throw new TestException("Got exception while getting pool connection",e);
   }
    return conn;
  }
}
