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

package io.snappydata.hydra.testDMLOps;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.Vector;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.test.util.TestException;

public class SnappyDMLOpsUsingAPITest extends SnappyDMLOpsUtil {

  protected static SnappyDMLOpsUsingAPITest apiTestInstance;

  /*
  * Hydra task to perform DMLOps which can be insert, update, delete
  */
  public static void HydraTask_performDMLOpsInAppUsingAPI() {
    apiTestInstance.performDMLOpUsingAPI(ConnType.SNAPPY);
  }

  public static void HydraTask_performDMLOpsInJobUsingAPI() {
    apiTestInstance.performDMLOpUsingAPI(ConnType.SMARTCONNECTOR);
  }

  public void performDMLOpUsingAPI(ConnType connType) {
    //perform DML operation which can be insert, update, delete.
    String operation = SnappySchemaPrms.getDMLOperations();
    switch (SnappyDMLOpsUtil.DMLOp.getOperation(operation)) {
      case INSERT:
        Log.getLogWriter().info("Performing insert operation.");
        performInsertUsingAPI(connType);
        break;
      case UPDATE:
        Log.getLogWriter().info("Performing update operation.");
        performUpdateUsingAPI(connType);
        break;
      case DELETE:
        Log.getLogWriter().info("Performing delete operation.");
        performDeleteUsingAPI(connType);
        break;
      default:
        Log.getLogWriter().info("Invalid operation. ");
        throw new TestException("Invalid operation type.");
    }
  }

  public void performUpdateUsingAPI(ConnType connType) {
    try {
      Connection dConn = null; //get the derby connection here
      String updateStmt[] = SnappySchemaPrms.getUpdateStmts();
      int rand = new Random().nextInt(updateStmt.length);
      String stmt = updateStmt[rand];
      String tableName = SnappySchemaPrms.getUpdateTables()[rand];
      int tid = getMyTid();
      if (testUniqueKeys) {
        if (stmt.toUpperCase().contains("WHERE"))
          stmt = stmt + " AND tid=" + tid;
        else stmt = stmt + " WHERE tid=" + tid;
      }

      String whereClause = "";
      String newValues = "";
      String columnToUpdate = "";
      if (connType.equals(ConnType.SNAPPY)) {
        Vector jobClass = new Vector<>();
        jobClass.addElement("io.snappydata.hydra.testDMLOps.ValidateUpdateOpUsingAPI_Job");
        dynamicAppProps.put(tid, "tableName=" + tableName + ",whereClause=" + whereClause + "," +
            "newValues=" + newValues + ",columnToUpdate=" + columnToUpdate);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(jobClass, logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        Vector jobClass = new Vector<>();
        jobClass.addElement("io.snappydata.hydra.testDMLOps.ValidateUpdateOpAppUsingAPI");
        dynamicAppProps.put(tid, tableName + " " + whereClause + " " + newValues + " " + columnToUpdate);
        String logFile = "snappyAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(jobClass, logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Updated " + derbyRows + " rows in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String selectQuery = SnappySchemaPrms.getAfterUpdateSelectStmts()[rand];
        String orderByClause = "";
//        String[] dmlTables = SnappySchemaPrms.getDMLTables();
//        orderByClause = SnappySchemaPrms.getOrderByClause()[Arrays.asList(dmlTables)
//            .indexOf(tableName)];
        String message = verifyResultsForTable(selectQuery, tableName, orderByClause, true);
        if (message.length() != 0) {
          throw new util.TestException("Validation failed after update table.");
        }
      }
    } catch (SQLException se) {
      throw new util.TestException("Got exception while performing update operation.", se);
    }
  }

  public void performDeleteUsingAPI(ConnType connType) {
    try {
      Connection dConn = null; //get the derby connection here
      String deleteStmt[] = SnappySchemaPrms.getDeleteStmts();
      int numRows = 0;
      int rand = new Random().nextInt(deleteStmt.length);
      String stmt = deleteStmt[rand];
      String tableName = SnappySchemaPrms.getDeleteTables()[rand];
      int tid = getMyTid();
      if (testUniqueKeys) {
        if (stmt.toUpperCase().contains("WHERE"))
          stmt = stmt + " AND tid=" + tid;
        else stmt = stmt + " WHERE tid=" + tid;
      }
      String whereClause = "";
      if (connType.equals(ConnType.SNAPPY)) {
        Vector jobClass = new Vector<>();
        jobClass.addElement("io.snappydata.hydra.testDMLOps.ValidateDeleteOpUsingAPI_Job");
        dynamicAppProps.put(tid, "tableName=" + tableName + ",whereClause=" + whereClause);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(jobClass, logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        Vector jobClass = new Vector<>();
        jobClass.addElement("io.snappydata.hydra.testDMLOps.ValidateDeleteOpAppUsingAPI");
        dynamicAppProps.put(tid, tableName + " " + whereClause);
        String logFile = "snappyAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(jobClass, logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Deleted " + derbyRows + " rows in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String selectQuery = SnappySchemaPrms.getAfterDeleteSelectStmts()[rand];
        String orderByClause = "";
//        String[] dmlTables = SnappySchemaPrms.getDMLTables();
//        orderByClause = SnappySchemaPrms.getOrderByClause()[Arrays.asList(dmlTables)
//            .indexOf(tableName)];
        String message = verifyResultsForTable(selectQuery, tableName, orderByClause, true);
        if (message.length() != 0) {
          throw new util.TestException("Validation failed after executing delete on table.");
        }
      }
    } catch (SQLException se) {
      throw new util.TestException("Got exception while performing delete operation.", se);
    }
  }

  public void performInsertUsingAPI(ConnType connType) {
    try {
      Connection dConn = null;
      String[] dmlTable = SnappySchemaPrms.getDMLTables();
      int rand = new Random().nextInt(dmlTable.length);
      String tableName = dmlTable[rand];
      String row = getRowFromCSV(tableName, rand);
      if (testUniqueKeys)
        row = row + "," + getMyTid();
      String stmt = SnappySchemaPrms.getInsertStmts()[rand];
      String insertStmt = getStmt(stmt, row, tableName);
      int tid = getMyTid();

      if (connType.equals(ConnType.SNAPPY)) {
        Vector jobClass = new Vector<>();
        jobClass.addElement("io.snappydata.hydra.testDMLOps.ValidateInsertOpUsingAPI_Job");
        dynamicAppProps.put(tid, "tableName=" + tableName + ",row=" + row);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(jobClass, logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        Vector jobClass = new Vector<>();
        jobClass.addElement("io.snappydata.hydra.testDMLOps.ValidateInsertOpAppUsingAPI");
        dynamicAppProps.put(tid, tableName + " " + row);
        String logFile = "snappyAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(jobClass, logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Inserting in derby : " + insertStmt);
        int derbyRowCount = dConn.createStatement().executeUpdate(insertStmt);
        Log.getLogWriter().info("Inserted " + derbyRowCount + " row in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String message = verifyResultsForTable("select * from ", tableName, "",true);
        if (message.length() != 0) {
          throw new util.TestException("Validation failed after insert in table " + tableName + ".");
        }
      }
    } catch (SQLException se) {
      throw new util.TestException("Got exception while performing insert operation.", se);
    }
  }

}
