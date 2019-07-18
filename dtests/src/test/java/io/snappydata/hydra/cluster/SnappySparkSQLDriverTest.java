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
package io.snappydata.hydra.cluster;

import hydra.Log;
import util.TestException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SnappySparkSQLDriverTest extends SnappyTest {

  public static void dropDBIssue() throws SQLException {
    Connection conn = getLocatorConnection();
    String query = "DROP DATABASE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8 CASCADE";
    Statement stmt = conn.createStatement();
    long startTime = System.currentTimeMillis();
    stmt.executeUpdate(query);
    //Thread.sleep(2000);
    stmt.cancel();
    long endTime = System.currentTimeMillis();
    Log.getLogWriter().info("Time to executed the query::  " + ((endTime - startTime) / 1000) + " s");
    closeConnection(conn);
  }

  public static void dropDBIssueConcCancel() {
    Connection conn;
    try {
      conn = getLocatorConnection();
      Statement stmt = conn.createStatement();
      Runnable dropDBCascade = () -> {
        try {
          String query = "DROP DATABASE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8 CASCADE";
          long startTime = System.currentTimeMillis();
          stmt.executeUpdate(query);
          long endTime = System.currentTimeMillis();
          Log.getLogWriter().info("Time to executed the query::  " + ((endTime - startTime) / 1000) + " s");
        } catch (SQLException se) {
          throw new TestException("Got exception while executing the query", se);
        }

      };

      Runnable cancleStmt = () -> {
        try {
          stmt.cancel();
        } catch (SQLException se) {
          throw new TestException("Got exception while cancelling the SQL Statement ", se);
        }
      };

      ExecutorService es = Executors.newFixedThreadPool(2);
      es.submit(dropDBCascade);
      es.submit(cancleStmt);
      es.shutdown();
      es.awaitTermination(180, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new TestException("Exception occurred while waiting for the snappy streaming job process execution." + "\nError Message:" + e.getMessage());
    } catch (SQLException se) {
      throw new TestException("Got exception while getting the connection", se);
    }
  }
}
