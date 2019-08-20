
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
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import util.TestException;

public class TomcatConnectionPool {

  public static TomcatConnectionPool connPoolInstance = null;
  public DataSource datasource;

  private TomcatConnectionPool() {
    Log.getLogWriter().info("Creating instance of TomcatConnectionPool");
    PoolProperties p = new PoolProperties();
    String url = SnappyConnectionPoolPrms.getUrl() + SnappyTest.validateLocatorEndpointData().get
        (0);
    p.setUrl(url);
    p.setDriverClassName(SnappyConnectionPoolPrms.getDriver());
    p.setUsername(SnappyConnectionPoolPrms.getUsername());
    p.setPassword(SnappyConnectionPoolPrms.getPassword());
    p.setInitialSize(SnappyConnectionPoolPrms.getInitialSize());
    p.setMaxWait(SnappyConnectionPoolPrms.getMaxWait());
    datasource = new DataSource();
    datasource.setPoolProperties(p);
  }

  public static TomcatConnectionPool getInstance(){
    if (connPoolInstance == null) 
      connPoolInstance = new TomcatConnectionPool();
    return connPoolInstance;
  }

  public static synchronized Connection getConnection() {
    TomcatConnectionPool connPool = TomcatConnectionPool.getInstance();
    Connection conn = null;
    try {
      conn = connPool.datasource.getConnection();
    }
    catch(Exception e) {
      Log.getLogWriter().info("Got exception while getting connection using tomcat connection " +
         "pool");
      throw new TestException("Got exception while getting pool connection",e);
    }
    return conn;
  }

}

