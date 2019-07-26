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

package io.snappydata.hydra.consistency;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import hydra.Log;
import hydra.blackboard.AnyCyclicBarrier;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.testDMLOps.DerbyTestUtils;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.hydra.testDMLOps.SnappySchemaPrms;
import util.TestException;

public class SnappyConsistencyTest extends SnappyDMLOpsUtil {

  public static String[] updateStmts = SnappySchemaPrms.getUpdateStmts();
  public static String[] deleteStmts = SnappySchemaPrms.getDeleteStmts();
  public static String[] insertStmts = SnappySchemaPrms.getInsertStmts();
  public static String[] putIntoStmts = SnappySchemaPrms.getPutIntoStmts();

  protected static SnappyConsistencyTest testInstance;

  public Random rand = new Random();

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    if(derbyTestUtils == null)
      derbyTestUtils = new DerbyTestUtils();
  }

  public static void HydraTask_performDMLOPsAndVerifyConsistency() {
    testInstance.performOps(ConnType.JDBC);
  }

  public static void HydraTask_performDMLOPsAndVerifyConsistencyInJob() {
    testInstance.performOps(ConnType.SNAPPY);
  }

  public static void HydraTask_performDMLOPsAndVerifyConsistencyInApp() {
    testInstance.performOps(ConnType.SMARTCONNECTOR);
  }

  public void performOps(ConnType conn) {
    int tid = getMyTid();
    String dmlSql, selectSql;
    String operation = SnappySchemaPrms.getDMLOperations();
    String tableName;
    int index;
    switch (DMLOp.getOperation(operation)) {
      case INSERT:
        index = rand.nextInt(insertStmts.length);
        dmlSql = insertStmts[index];
        selectSql = SnappySchemaPrms.getAfterInsertSelectStmts()[index];
        tableName = SnappySchemaPrms.getInsertTables()[index];
        break;
      case UPDATE:
        index = rand.nextInt(updateStmts.length);
        dmlSql = updateStmts[index];
        selectSql = SnappySchemaPrms.getAfterUpdateSelectStmts()[index];
        tableName = SnappySchemaPrms.getUpdateTables()[index];
        break;
      case DELETE:
        index = rand.nextInt(deleteStmts.length);
        dmlSql = deleteStmts[index];
        selectSql = SnappySchemaPrms.getAfterDeleteSelectStmts()[index];
        tableName = SnappySchemaPrms.getDeleteTables()[index];
        break;
      case PUTINTO:
        index = rand.nextInt(putIntoStmts.length);
        dmlSql = putIntoStmts[index];
        selectSql = SnappySchemaPrms.getAfterPutIntoSelectStmts()[index];
        tableName = SnappySchemaPrms.getInsertTables()[index];
        break;
      default:
        Log.getLogWriter().info("Got invalid operation " + operation);
        throw new TestException("Invalid operation type " + operation);
    }
    int batchSize = SnappySchemaPrms.getBatchSize();
    index = Arrays.asList(SnappySchemaPrms.getTableNames()).indexOf(tableName);

    if(operation.equalsIgnoreCase("delete") && dmlSql.contains("$counter")){
      int delCounter = getDeleteCounter(index,batchSize);
      dmlSql = dmlSql.replace("$counter",delCounter+ "");
      selectSql = selectSql.replace("$counter", delCounter + "");
    }

    if (SnappySchemaPrms.isTestUniqueKeys() ) {
      if (!(operation.equalsIgnoreCase("insert") || operation.equalsIgnoreCase("putinto")))
        dmlSql = addTidToQuery(dmlSql, tid);
      selectSql = addTidToQuery(selectSql, tid);
    }
    if (conn.equals(ConnType.JDBC)) {
      ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> future1 =  pool.submit(new DMLOpsThread(tid, operation, batchSize, dmlSql,
            index, tableName));
        Future<?> future2 =  pool.submit(new SelectOpsThread(tid, selectSql, operation, tableName
            , batchSize));
        try{
          future2.get();
          future1.get();
      } catch (Exception e) {
        throw new TestException("Got Exception while performing operation " + operation + " : ", e);
      }
      pool.shutdown();
      try {
        pool.awaitTermination(120, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        Log.getLogWriter().info("Got exception while waiting for all threads to complete the " +
            "tasks");
      }
    } else {
      if(dmlSql.contains("$tid")) dmlSql = dmlSql.replace("$tid", tid + "");
      int initCounter = getInitialCounter(index,batchSize);
      if(dmlSql.contains("$range")){
        dmlSql = dmlSql.replace("$range",  initCounter + "," + (initCounter + batchSize));
      }
      if (conn.equals(ConnType.SNAPPY)) {
        String app_props = "tid=" + tid;
        app_props += ",operation=" + operation;
        app_props += ",batchsize=" + batchSize;
        app_props += ",tableName=" + tableName;
        app_props += ",selectStmt=\\\"" + selectSql + "\\\"";
        app_props += ",dmlStmt=\\\"" + dmlSql + "\\\"";
        dynamicAppProps.put(tid, app_props);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile,
            SnappyPrms.getUserAppJar(), jarPath, SnappyPrms.getUserAppName());
      } else if (conn.equals(ConnType.SMARTCONNECTOR)) {
        String app_props = tid + " " + operation + " " + batchSize + " " + tableName +
            " \"" + selectSql + "\" \"" + dmlSql + "\"";
        dynamicAppProps.put(tid, app_props);
        String logFile = "sparkAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
    }
    if (hasDerbyServer) {
      if (!((operation.equalsIgnoreCase("insert") || operation.equalsIgnoreCase("putinto"))
          && conn.equals(ConnType.JDBC))) {
        Log.getLogWriter().info("Performing operation in derby..");
        try {
          Connection dConn = derbyTestUtils.getDerbyConnection();
          dConn.createStatement().execute(dmlSql);
          derbyTestUtils.closeDiscConnection(dConn, true);
        } catch (SQLException se) {
          throw new TestException("Got exception while executing operations on derby", se);
        }
      }
    }
  }

  class DMLOpsThread implements Runnable{
    int tid;
    String operation;
    int batchSize;
    String dmlStmt;
    int index;
    String tableName;

    public DMLOpsThread(int tid, String operation, int batchSize, String dmlStmt,
        int index, String tableName ) {
      this.tid = tid;
      this.operation = operation;
      this.batchSize = batchSize;
      this.dmlStmt = dmlStmt;
      this.index = index;
      this.tableName = tableName;
    }

    public void run() {
      try {
        performDMLOp(tid, operation, batchSize, dmlStmt, tableName, index);
      } catch (Exception te){
        Log.getLogWriter().info("Got exception while executing " + operation + ". Exception " +
            "is :", te);
        throw new TestException("Got exception while executing " + operation + ". Exception is :"
            , te);
      }
    }
  }

  public void performDMLOp(int tid, String operation, int batchSize, String stmt,
      String tableName, int index) {
    Connection conn = null;
    try {
      conn = getLocatorConnection();
      if(operation.equalsIgnoreCase("insert") || operation.equalsIgnoreCase("putinto"))
        performInsertUsingBatch(conn, tableName, stmt, index, batchSize, tid, false);
      else {
        waitForBarrier(tid + "", 2);
        int numRows = conn.createStatement().executeUpdate(stmt);
        operation = operation.toUpperCase();
        Log.getLogWriter().info(
            operation.equals("INSERT")? operation + "ED":
            operation.equals("PUTINTO")?"UPDATED":operation +"D"
                + " " + numRows + " rows.");
      }
      conn.close();
    } catch (SQLException se) {
      throw new TestException("Got SQLException while executing " +  operation + ".", se);
    } finally{
      try {
        conn.close();
      } catch (SQLException se){
        throw new TestException("Got exception while closing connection");
      }
    }
  }

  class SelectOpsThread implements Runnable {
    int tid;
    String sql;
    String operation;
    String tableName;
    int batchSize;
;

    public SelectOpsThread(int tid, String sql, String operation, String tableName, int batchSize) {
      this.tid = tid;
      this.sql = sql;
      this.operation = operation;
      this.batchSize = batchSize;
      this.tableName = tableName;
    }

    public void run() {
      try {
        executeSelect(tid, sql, operation, tableName, batchSize);
      } catch (Exception e) {
        Log.getLogWriter().info("Got exception while executing " + operation + ". Exception " +
            "is :" + e);
        throw new TestException("Got exception while executing " + operation + ". Exception is :"
            , e);
      }
    }
  }

  public void executeSelect(int tid, String query, String dmlOp, String tableName,
      int batchSize) {
    Connection conn = null;

    Log.getLogWriter().info("Executing select " + query +  " for " + dmlOp + " operation.");
    try {
      conn = getLocatorConnection();
      Log.getLogWriter().info("Executing query :" + query);
      ResultSet beforeDMLRS = conn.createStatement().executeQuery(query);
      waitForBarrier(tid + "", 2);
      ResultSet afterDMLRS = conn.createStatement().executeQuery(query);
      conn.close();
      Log.getLogWriter().info("Verifying the results for atomicity..");
      if (verifyAtomicity(beforeDMLRS, afterDMLRS, dmlOp, tableName, batchSize)) {
        Log.getLogWriter().info("Test failed to get atomic data during " + dmlOp + ".");
        throw new TestException("Test failed to get atomic data during " + dmlOp + ".");
      }
    } catch (SQLException se) {
      Log.getLogWriter().info("Got exception while executing select query", se);
      throw new TestException("Got exeception while executing select query for " + dmlOp + ".", se);
    } finally{
      try {
        conn.close();
      } catch (SQLException se){
        throw new TestException("Got exception while closing connection",se);
      }
    }
  }

  public boolean verifyAtomicity(ResultSet beforeDMLRS, ResultSet afterDMLRS, String op,
      String tableName,
   int batchSize) {
    boolean atomicityCheckFailed = false;
    int before_result = 0, after_result = 0;
    int defaultValue;
    try {
      ResultSetMetaData rsmd = beforeDMLRS.getMetaData();
      int queryColCnt = rsmd.getColumnCount();

      while (beforeDMLRS.next()) {
        afterDMLRS.next();
        int rowCount = 0;
        for(int i = 1 ; i<= queryColCnt ; i++) {
          String colName = rsmd.getColumnLabel(i);
          before_result = beforeDMLRS.getInt(i);
          after_result = afterDMLRS.getInt(i);
          Log.getLogWriter().info(colName + " in table " + tableName + " before " + op +
              " start: " + before_result + " and " + colName + " after " + op + " start : " +
              after_result);
          switch (DMLOp.getOperation(op)) {
            case INSERT:
              defaultValue = -1;
              if (colName.toUpperCase().startsWith("COUNT")) {
                rowCount = before_result;
                int expectedRs = after_result - before_result;
                if (!(expectedRs == 0 || expectedRs == batchSize)) {
                  atomicityCheckFailed = true;
                }
              } else if (colName.toUpperCase().startsWith("AVG")) {
                int expectedRs =
                    ((before_result * rowCount) + (defaultValue * batchSize))/(rowCount+batchSize);
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true;
                }
              } else if (colName.toUpperCase().startsWith("SUM")) {
                int expectedRs = before_result + (defaultValue * batchSize);
                if (!(after_result == before_result || after_result == expectedRs))
                  atomicityCheckFailed = true;
              }
              break;
            case UPDATE:
              defaultValue = 1;
              if (colName.toUpperCase().startsWith("COUNT")) {
                int expectedRs = after_result - before_result;
                if (!(expectedRs == 0)) {
                  atomicityCheckFailed = true;
                }
              } else if (colName.toUpperCase().startsWith("AVG")) {
                int expectedRs = before_result + defaultValue;
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true;
                }
              } else if (colName.toUpperCase().startsWith("SUM")) {
                int expectedRs = before_result + (defaultValue * batchSize);
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true;
                }
              } break;
            case DELETE:
              defaultValue = 0;
              if (colName.toUpperCase().startsWith("COUNT")) {
                int expectedRs = after_result - before_result;
                if (!(expectedRs % before_result == 0))
                  atomicityCheckFailed = true;
              } else if (colName.toUpperCase().startsWith("AVG")) {
                int expectedRs = after_result - before_result;
                if (!(expectedRs % before_result == 0))
                  atomicityCheckFailed = true;
              } else if (colName.toUpperCase().startsWith("SUM")) {
                  int expectedRs = after_result - before_result;
                  if (!(expectedRs % before_result == 0))
                    atomicityCheckFailed = true;
              }break;
            case PUTINTO:
              defaultValue = -1;
              if (colName.toUpperCase().startsWith("COUNT")) {
                rowCount = before_result;
                int expectedRs = after_result - before_result;
                if (!(expectedRs % batchSize == 0)) {
                  atomicityCheckFailed = true;
                }
              } else if (colName.toUpperCase().startsWith("AVG")) {
                int expectedRs =
                    ((before_result * rowCount) + (defaultValue * batchSize))/(rowCount+batchSize);
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true;
                }
              } else if (colName.toUpperCase().startsWith("SUM")) {
                int expectedRs = before_result + (defaultValue * batchSize);
                if (!(after_result == before_result || after_result == expectedRs)) {
                  atomicityCheckFailed = true;
                }
              } break;
            default:
          }
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while accessing resultset.", se);
    }
    if(atomicityCheckFailed) return true;
    return false;
  }

  public static void waitForBarrier(String barrierName, int numThreads) {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, barrierName);
    Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier " + barrierName);
    barrier.await();
    Log.getLogWriter().info("Wait completed for " + barrierName);
  }

}