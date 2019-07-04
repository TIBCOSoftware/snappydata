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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import hydra.Log;
import hydra.blackboard.AnyCyclicBarrier;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.testDMLOps.DerbyTestUtils;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.hydra.testDMLOps.SnappySchemaPrms;
import util.TestException;

public class SnappyConsistencyTest extends SnappyDMLOpsUtil {

  public static boolean testUniqueKeys = false;
  public static int batchSize = SnappySchemaPrms.getBatchSize();
  protected static hydra.blackboard.SharedLock BBLock;
  public static DerbyTestUtils derbyTestUtils;

  public static String[] updateStmts = SnappySchemaPrms.getUpdateStmts();
  public static String[] deleteStmts = SnappySchemaPrms.getDeleteStmts();
  public static String[] insertStmts = SnappySchemaPrms.getInsertStmts();
  public static String[] putIntoStmts = SnappySchemaPrms.getPutIntoStmts();

  protected static SnappyConsistencyTest testInstance;

  public Random rand = new Random();

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    derbyTestUtils = new DerbyTestUtils();
  }

  public static void HydraTask_populateTables() {
    testInstance.populateTables();
  }

  public void populateTables(){
    batchSize = 1000000;
    ArrayList<Integer> dmlThreads = null;
    if (SnappyBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlThreads = (ArrayList<Integer>) SnappyBB.getBB().getSharedMap().get("dmlThreads");
    try {
      Connection conn = getLocatorConnection();
      for(int i =0; i<dmlThreads.size() ; i++)
        performInsert(dmlThreads.get(i), conn);
    } catch (SQLException se) {

    }

  }

  public void executeSelect(int tid) {
    Connection conn = null;
    String dmlOp = "";
    String query = "";
    dmlOp = getDMLOpForThrFromBB(tid);
    switch (DMLOp.getOperation(dmlOp)) {
      case INSERT:
        query = SnappySchemaPrms.getAfterInsertSelectStmts()[getOpNumForThrFromBB(tid)];
        break;
      case UPDATE:
        query = SnappySchemaPrms.getAfterUpdateSelectStmts()[getOpNumForThrFromBB(tid)];
        break;
      case DELETE:
        query = SnappySchemaPrms.getAfterDeleteSelectStmts()[getOpNumForThrFromBB(tid)];
        break;
      case PUTINTO:
        query = SnappySchemaPrms.getAfterPutIntoSelectStmts()[getOpNumForThrFromBB(tid)];
        break;
      default:
        Log.getLogWriter().info("Invalid operation. ");
        throw new TestException("Invalid operation type.");
    }
    Log.getLogWriter().info("Executing select for " + dmlOp + " operation.");
    if (SnappySchemaPrms.isTestUniqueKeys()) {
      if (!query.contains("where"))
        query = query + " WHERE ";
      else query = query + " AND ";
      query = query + " tid = " + tid;
    }
    try {
      conn = getLocatorConnection();
      Log.getLogWriter().info("Executing query :" + query);
      ResultSet beforeDMLRS = conn.createStatement().executeQuery(query);
      waitForBarrier(tid + "", 2);
      ResultSet afterDMLRS = conn.createStatement().executeQuery(query);
      conn.close();
      Log.getLogWriter().info("Verifying the results for atomicity..");
      int before_result = 0, after_result = 0;
      while (beforeDMLRS.next()) {
        before_result = beforeDMLRS.getInt(1);
      }
      while (afterDMLRS.next()) {
        after_result = afterDMLRS.getInt(1);
      }
      if (!verifyAtomicity(before_result, after_result, dmlOp)) {
        throw new TestException("Test failed to get atomic data during DML operations");
      }
    } catch (SQLException se) {
      Log.getLogWriter().info("Got exception while executing select query", se);
    }
  }

  public static boolean verifyAtomicity(int rs_before, int rs_after, String op) {
    switch(DMLOp.getOperation(op)) {
      case INSERT:
      case DELETE:
      case PUTINTO:
        Log.getLogWriter().info("Number of rows before DML start: " + rs_before + " and number of " +
            "after DML start : " + rs_after);
        if (rs_after % batchSize == 0)
          return true;
        break;
      case UPDATE:
        Log.getLogWriter().info("Avg before update: " + rs_before + " and Avg after update " +
            "started  : " + rs_after);
        if (rs_after % 5 == 0)
          return true;
        break;
    }
    return false;
  }

  public String addTidToQuery(String sql, int tid){
    if (!sql.contains("where"))
      sql = sql + " WHERE ";
    else sql = sql + " AND ";
    sql = sql + " tid = " + tid;
    return sql;
  }

  public static void HydraTask_performDMLOPsAndVerifyConsistency() {
    testInstance.performOpsAndVerifyConsistency();
  }

  public static void HydraTask_performDMLOPsAndVerifyConsistencyInJob() {
    testInstance.performOpsUsingSnappyJob(ConnType.SNAPPY);
  }

  public static void HydraTask_performDMLOPsAndVerifyConsistencyInApp() {
    testInstance.performOpsUsingSnappyJob(ConnType.SMARTCONNECTOR);
  }

  public void performOpsUsingSnappyJob(ConnType conn) {
    int tid = getMyTid();
    String dmlSql, selectSql;
    String operation = SnappySchemaPrms.getDMLOperations();
    int index;
    switch (DMLOp.getOperation(operation)) {
      case INSERT:
        index = rand.nextInt(insertStmts.length);
        dmlSql = insertStmts[index];
        selectSql = SnappySchemaPrms.getAfterInsertSelectStmts()[index];
        break;
      case UPDATE:
        index = rand.nextInt(insertStmts.length);
        dmlSql = insertStmts[index];
        selectSql = SnappySchemaPrms.getAfterInsertSelectStmts()[index];
        break;
      case DELETE:
        index = rand.nextInt(insertStmts.length);
        dmlSql = insertStmts[index];
        selectSql = SnappySchemaPrms.getAfterInsertSelectStmts()[index];
        break;
      case PUTINTO:
        index = rand.nextInt(insertStmts.length);
        dmlSql = insertStmts[index];
        selectSql = SnappySchemaPrms.getAfterInsertSelectStmts()[index];
        break;
      default:
        Log.getLogWriter().info("Invalid operation. ");
        throw new TestException("Invalid operation type.");
    }
    if (SnappySchemaPrms.isTestUniqueKeys()) {
      dmlSql = addTidToQuery(dmlSql,tid);
      selectSql = addTidToQuery(selectSql,tid);
    }
    String app_props;
    if (conn.equals(ConnType.SNAPPY)) {
      app_props = "tid=" + tid;
      app_props += ",operation=" + operation;
      app_props += ",batchsize=" + batchSize;
      app_props += ",selectStmt=\\\"" + selectSql + "\\\"";
      app_props += ",dmlStmt=\\\"" + dmlSql + "\\\"";
      dynamicAppProps.put(tid, app_props);
      String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
      executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile,
          SnappyPrms.getUserAppJar(), jarPath, SnappyPrms.getUserAppName());
    } else if (conn.equals(ConnType.SMARTCONNECTOR)) {
      app_props =
          tid + " " + operation + " " + batchSize + " \"" + selectSql + "\" \"" + dmlSql + "\"";
      dynamicAppProps.put(tid, app_props);
      String logFile = "sparkAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
      executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
    }
    try {
      Connection dConn = derbyTestUtils.getDerbyConnection();
      dConn.createStatement().execute(dmlSql);
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch(SQLException se) {
      throw new TestException("Got exception while executing operations on derby", se);
    }
  }

  public void performOpsAndVerifyConsistency() {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    int tid = getMyTid();
    pool.execute(new DMLOpsThread(tid));
    pool.execute(new SelectOpsThread(tid));
  }

  class DMLOpsThread implements Runnable{
    int tid;

    public DMLOpsThread(int tid) {
      this.tid = tid;
    }

    public void run() {
      String operation = SnappySchemaPrms.getDMLOperations();
      registerOperationInBB(tid, operation);
      performDMLOp(tid, operation);
    }
  }

  class SelectOpsThread implements Runnable {
    int tid;

    public SelectOpsThread(int tid) {
      this.tid = tid;
    }

    public void run(){
      executeSelect(tid);
    }
  }

  public void performDMLOp(int tid, String operation){
    Connection conn;
    try{
      conn = getLocatorConnection();
    } catch (SQLException se) {
      throw new TestException("Got exception while obtaining jdbc connection.", se);
    }
    switch (DMLOp.getOperation(operation)) {
      case INSERT:
        Log.getLogWriter().info("Performing insert operation.");
        performInsert(tid, conn); break;
      case UPDATE:
        Log.getLogWriter().info("Performing update operation.");
        performUpdate(tid, conn); break;
      case DELETE:
        Log.getLogWriter().info("Performing delete operation.");
        performDelete(tid, conn); break;
      case PUTINTO:
        Log.getLogWriter().info("Performing putinto operation...");
        performPutInto(tid, conn); break;
      default:
        Log.getLogWriter().info("Invalid operation. ");
        throw new TestException("Invalid operation type.");
    }
    try {
      conn.close();
    } catch(SQLException se) {
      throw new TestException("Got Exception whiling closing jdbc connection.", se);
    }
  }


  public void performInsert(int tid, Connection conn) {
    String query = "SELECT max(id) FROM table1";
    int maxID = 0;
    ArrayList<Integer> dmlThreads = null;
    try {
      ResultSet rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {
        maxID = rs.getInt(1);
      }

      int index = rand.nextInt(insertStmts.length);
      String sql = insertStmts[index];
      registerOpNumInBB(tid, index);
      PreparedStatement ps = conn.prepareStatement(sql);

      Connection dConn = derbyTestUtils.getDerbyConnection();
      PreparedStatement psDerby = dConn.prepareStatement(sql);

      for (int i = 0; i < batchSize; i++) {
        ps.setInt(1, maxID++);
        ps.setString(2, "name" + maxID);
        ps.setInt(3, -1);
        ps.setInt(4, tid);
        ps.addBatch();

        psDerby.setInt(1, maxID++);
        psDerby.setString(2, "name" + maxID);
        psDerby.setInt(3, -1);
        psDerby.setInt(4, tid);
        psDerby.addBatch();
      }
      waitForBarrier("" + tid, 2);
      ps.executeBatch();
      Log.getLogWriter().info("Inserted " + batchSize + " rows in the snappy table.");
      psDerby.executeBatch();
      derbyTestUtils.closeDiscConnection(dConn, true);

    } catch (SQLException se) {
      throw new TestException("Caught Exception while performing bulk inserts", se);
    }
  }

  public void performUpdate(int tid, Connection conn) {
    int index = rand.nextInt(updateStmts.length);
    String sql = updateStmts[index];
    registerOpNumInBB(tid, index);
    if(SnappySchemaPrms.isTestUniqueKeys())
      sql= addTidToQuery(sql, tid);
    Log.getLogWriter().info("Performing batch update in snappy..");
    waitForBarrier("" + tid,2);
    try {
      conn.createStatement().execute(sql);
      Log.getLogWriter().info("Performing batch update in derby..");
      Connection dConn = derbyTestUtils.getDerbyConnection();
      dConn.createStatement().execute(sql);
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing batch update", se);
    }
  }

  public void performDelete(int tid, Connection conn) {
    int index = rand.nextInt(deleteStmts.length);
    String sql = deleteStmts[index];
    registerOpNumInBB(tid, index);;
    if(SnappySchemaPrms.isTestUniqueKeys())
      sql= addTidToQuery(sql, tid);
    Log.getLogWriter().info("Performing delete in snappy..");
    waitForBarrier("" + tid,2);
    try {
      conn.createStatement().execute(sql);
      Log.getLogWriter().info("Performing delete in derby..");
      Connection dConn = derbyTestUtils.getDerbyConnection();
      dConn.createStatement().execute(sql);
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing bulk delete", se);
    }
  }

  public void performPutInto(int tid, Connection conn) {
    int index = rand.nextInt(putIntoStmts.length);
    String sql = putIntoStmts[index];
    registerOpNumInBB(tid, index);
    waitForBarrier("" + tid ,2);
    try {
      conn.createStatement().execute(sql);
      Log.getLogWriter().info("Performing delete in derby..");
      Connection dConn = derbyTestUtils.getDerbyConnection();
      dConn.createStatement().execute(sql);
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing bulk delete", se);
    }
  }

  /*
  Register the dml operation performed by the thread in BB, so that select thread will know which
   select is to be performed.
  */
  public void registerOperationInBB(int tid, String operation){
    SnappyBB.getBB().getSharedMap().put("op_" + tid,operation);
  }

  public String getDMLOpForThrFromBB(int tid) {
    String operation = "";
    try {
      while (!SnappyBB.getBB().getSharedMap().containsKey("op_" + tid)) Thread.sleep(10);
    } catch (InterruptedException ie) {
      Log.getLogWriter().info("Got interrupted exception");
    }
    operation = (String) SnappyBB.getBB().getSharedMap().get("op_" + tid);
    SnappyBB.getBB().getSharedMap().remove("op_" + tid);
    return operation;
  }

  /*
  Register the number of dml operation performed by the thread in BB, so that select thread will
  know which select is to be performed.
  */
  public void registerOpNumInBB(int tid, int opNum){
    SnappyBB.getBB().getSharedMap().put("opNum_" + tid,opNum);
  }

  public int getOpNumForThrFromBB(int tid) {
    int opNum;
    try {
      while (!SnappyBB.getBB().getSharedMap().containsKey("opNum_" + tid)) Thread.sleep(10);
    } catch (InterruptedException ie) {
      Log.getLogWriter().info("Got interrupted exception");
    }
    opNum = (int) SnappyBB.getBB().getSharedMap().get("opNum_" + tid);
    SnappyBB.getBB().getSharedMap().remove("opNum_" + tid);
    return opNum;
  }

  protected void waitForBarrier(String barrierName, int numThreads) {
    if(!SnappySchemaPrms.getIsSingleBucket()) {
      AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, barrierName);
      Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier");
      barrier.await();
      Log.getLogWriter().info("Wait Completed...");
    }
  }


}
