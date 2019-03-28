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

import hydra.Log;
import hydra.blackboard.AnyCyclicBarrier;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.DerbyTestUtils;
import io.snappydata.hydra.testDMLOps.SnappySchemaPrms;
import util.TestException;

public class SnappyConsistencyTest extends SnappyTest {

  public static boolean testUniqueKeys = false;
  public static int batchSize = 1000;
  protected static hydra.blackboard.SharedLock BBLock;
  public static DerbyTestUtils derbyTestUtils;

  public String updateStmt[] = {
      "update table1 set code = case when code=-1 then 0 else (code+5) end ",
  };

  public String selectStmt[] = {
      "select count(*) from table1",
      "select avg(code) from table1"
  };

  public String insertStmt[] = {
      "insert into table1 values (?,?,?,?)"
  };


  public String deleteStmt[] = {
      "delete from table1 ",
   };

  protected static SnappyConsistencyTest testInstance;

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    derbyTestUtils = new DerbyTestUtils();
  }

  public static void HydraTask_populateTables() {
    testInstance.populateTables();
  }

  public void populateTables(){
    batchSize = 65000;
    try {
      Connection conn = getLocatorConnection();
      for(int i =0; i<10 ; i++)
        performBulkInsert(conn, true);
    } catch (SQLException se) {

    }
  }

  public static void HydraTask_registerDMLThreads(){
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    testInstance.getBBLock();
    ArrayList<Integer> dmlThreads;
    if (SnappyBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlThreads = (ArrayList<Integer>)SnappyBB.getBB().getSharedMap().get("dmlThreads");
    else
      dmlThreads = new ArrayList<>();
    if (!dmlThreads.contains(testInstance.getMyTid())) {
      dmlThreads.add(testInstance.getMyTid());
      SnappyBB.getBB().getSharedMap().put("dmlThreads", dmlThreads);
    }
    testInstance.releaseDmlLock();
  }

  public static void HydraTask_registerSelectThreads(){
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    testInstance.getBBLock();
    ArrayList<Integer> selectThreads;
    if (SnappyBB.getBB().getSharedMap().containsKey("selectThreads"))
      selectThreads = (ArrayList<Integer>)SnappyBB.getBB().getSharedMap().get("selectThreads");
    else
      selectThreads = new ArrayList<>();
    if (!selectThreads.contains(testInstance.getMyTid())) {
      selectThreads.add(testInstance.getMyTid());
      SnappyBB.getBB().getSharedMap().put("selectThreads", selectThreads);
    }
    testInstance.releaseDmlLock();
  }

  protected void getBBLock() {
    if (BBLock == null)
      BBLock = SnappyBB.getBB().getSharedLock();
    BBLock.lock();
  }

  protected void releaseDmlLock() {
    BBLock.unlock();
  }


  public static void HydraTask_executeQueries() {
    testInstance.executeSelect();
  }

  public void executeSelect() {
    Connection conn = null;
    int tid = getDMLThread();
    String dmlOp = getOperationForDMLThr(tid);
    Log.getLogWriter().info("DML op is : " + dmlOp);
    String query = "";
    if (dmlOp.equalsIgnoreCase("insert") || dmlOp.equalsIgnoreCase("delete"))
      query = selectStmt[0];
    else
      query = selectStmt[1];
    if(SnappySchemaPrms.isTestUniqueKeys()){
      if(!query.contains("where"))
        query = query + " where ";
      else query = query + " and ";
      query = query + " tid = " +  tid;
    }
    try {
      conn = getLocatorConnection();
      Log.getLogWriter().info("Executing query :" + query);
      ResultSet beforeDMLRS = conn.createStatement().executeQuery(query);
      waitForBarrier("" + tid, 2);
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

  public String getOperationForDMLThr(int tid) {
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

  public int getDMLThread() {
    int myTid = getMyTid();
    ArrayList<Integer> dmlThreads = (ArrayList<Integer>) SnappyBB.getBB().getSharedMap().get("dmlThreads");
    ArrayList<Integer> selectThreads = (ArrayList<Integer>) SnappyBB.getBB().getSharedMap().get("selectThreads");
    int index = selectThreads.indexOf(myTid);
    return dmlThreads.get(index);
  }

  public static boolean verifyAtomicity(int rs_before, int rs_after, String op) {
    Log.getLogWriter().info("Number of rows before DML start: " + rs_before + " and number of " +
        "after DML start : " + rs_after);
    if((rs_before == rs_after) || (rs_before %1000 == 0))
      return true;
    return false;
  }

  public enum DMLOp {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete"),
    PUTINTO("putinto");

    String opType;
    DMLOp(String opType) {
      this.opType = opType;
    }

    public String getOpType (){
      return opType;
    }

    public static DMLOp getOperation(String dmlOp) {
      if (dmlOp.equals(INSERT.getOpType())) {
        return INSERT;
      }else if (dmlOp.equals(UPDATE.getOpType())) {
        return UPDATE;
      } else if (dmlOp.equals(DELETE.getOpType())) {
        return DELETE;
      } else if(dmlOp.equals(PUTINTO.getOpType())) {
        return PUTINTO;
      }
      else return null;
    }
  }

  public static void HydraTask_performDMLOps() {
    testInstance.performDMLOp();
  }

  public void performDMLOp() {
    try {
      Connection conn = getLocatorConnection();
      //perform DML operation which can be insert, update, delete.
      String operation = SnappySchemaPrms.getDMLOperations();
      registerOperationInBB(operation);
      switch (DMLOp.getOperation(operation)) {
        case INSERT:
          Log.getLogWriter().info("Performing insert operation...");
          performBulkInsert(conn, false);
          break;
        case UPDATE:
          Log.getLogWriter().info("Performing update operation...");
          performBatchUpdate(conn);
          break;
        case DELETE:
          Log.getLogWriter().info("Performing delete operation...");
          performBulkDelete(conn);
          break;
        case PUTINTO:
          Log.getLogWriter().info("Performing putinto operation...");
          performPutInto(conn);
        default: Log.getLogWriter().info("Invalid operation. ");
          throw new TestException("Invalid operation type.");
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing DML Ops. Exception is : " ,se);
    }
  }

  public void registerOperationInBB(String operation){
    SnappyBB.getBB().getSharedMap().put("op_" + getMyTid(),operation);
  }

  public void performBulkInsert(Connection conn, boolean isPopulate) {
    int tid = getMyTid();
    String query = "select max(id) from table1";
    int maxID = 0;
    ArrayList<Integer> dmlThreads = null;
    if(isPopulate) {
      if (SnappyBB.getBB().getSharedMap().containsKey("dmlThreads"))
        dmlThreads = (ArrayList<Integer>) SnappyBB.getBB().getSharedMap().get("dmlThreads");
      tid = dmlThreads.get(new Random().nextInt(dmlThreads.size()));
    }
    Connection dConn = derbyTestUtils.getDerbyConnection();
    try {
      ResultSet rs = conn.createStatement().executeQuery(query);
      while(rs.next()) {
        maxID = rs.getInt(1);
      }
      PreparedStatement ps = conn.prepareStatement(insertStmt[0]);
      PreparedStatement psDerby = dConn.prepareStatement(insertStmt[0]);
      for(int i = 0; i<batchSize ; i++) {
        ps.setInt(1, maxID++);
        ps.setString(2,"name" + maxID);
        ps.setInt(3, -1);
        ps.setInt(4, tid);
        ps.addBatch();

        psDerby.setInt(1, maxID++);
        psDerby.setString(2,"name" + maxID);
        psDerby.setInt(3, -1);
        psDerby.setInt(4, tid);
        psDerby.addBatch();
      }
      if(!isPopulate)
        waitForBarrier("" + tid,2);
      ps.executeBatch();
      Log.getLogWriter().info("Inserted " + batchSize + " rows in the snappy table.");
      psDerby.executeBatch();
      derbyTestUtils.closeDiscConnection(dConn, true);

    } catch( SQLException se) {
      throw new TestException("Caught Exception while performing bulk inserts", se);

    }
  }

  public void performBatchUpdate(Connection conn) {
    String query = updateStmt[0];
    int tid = getMyTid();
    Connection dConn = derbyTestUtils.getDerbyConnection();
    Log.getLogWriter().info("Performing batch update in snappy..");
    waitForBarrier("" + tid,2);
    try {
      conn.createStatement().execute(query);
      Log.getLogWriter().info("Performing batch update in derby..");
      dConn.createStatement().execute(query);
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing batch update", se);
    }


  }

  public void performBulkDelete(Connection conn) {
    String query = deleteStmt[0];
    int tid = getMyTid();
    Connection dConn = derbyTestUtils.getDerbyConnection();
    Log.getLogWriter().info("Performing delete in snappy..");
    waitForBarrier("" + tid,2);
    try {
      conn.createStatement().execute(query);
      Log.getLogWriter().info("Performing delete in derby..");
      dConn.createStatement().execute(query);
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing bulk delete", se);
    }
  }

  public void performPutInto(Connection conn) {
    int tid = getMyTid();
    waitForBarrier("" + tid ,2);
  }

  /*
  protected void waitForBarrier(int numThreads) {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, "barrier");
    Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier");
    barrier.await();
  }*/

  protected void waitForBarrier(String barrierName, int numThreads) {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, barrierName);
    Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier");
    barrier.await();
  }


}
