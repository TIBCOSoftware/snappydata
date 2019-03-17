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

import hydra.Log;
import hydra.blackboard.AnyCyclicBarrier;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms;
import io.snappydata.hydra.testDMLOps.SnappySchemaPrms;
import util.TestException;

public class SnappyConsistencyTest extends SnappyTest {

  public static boolean testUniqueKeys = false;
  public static int batchSize = 1000;
  protected static hydra.blackboard.SharedLock BBLock_DML,BBLock_select;

  public String updateStmt[] = {
      "update tab1 set code = case when coded=-1 then 0 else (coded+5) end;",
  };

  public String selectStmt[] = {
      "select count(*) from tab1",
      "select avg(code) from tab1"
  };

  public String insertStmt[] = {
      "insert into tab1 values (?,?,?,?)"
  };


  public String deleteStmt[] = {
      "delete from tab1 where tid = ?",
   };

  protected static SnappyConsistencyTest testInstance;

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
  }

  public static void HydraTask_populateTables() {
    testInstance.populateTables();
  }

  public void populateTables(){
    batchSize = 65000;
    Connection conn = getConnection();
    performBulkInsert(conn);
  }

  public static void HydraTask_registerDMLThreads(){
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    testInstance.getBBLock(BBLock_DML);
    ArrayList<Integer> dmlThreads;
    if (SnappyBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlThreads = (ArrayList<Integer>)SnappyBB.getBB().getSharedMap().get("dmlThreads");
    else
      dmlThreads = new ArrayList<>();
    if (!dmlThreads.contains(testInstance.getMyTid())) {
      dmlThreads.add(testInstance.getMyTid());
      SnappyBB.getBB().getSharedMap().put("dmlThreads", dmlThreads);
    }
    testInstance.releaseDmlLock(BBLock_DML);
  }

  public static void HydraTask_registerSelectThreads(){
    if (testInstance == null)
      testInstance = new SnappyConsistencyTest();
    testInstance.getBBLock(BBLock_select);
    ArrayList<Integer> selectThreads;
    if (SnappyBB.getBB().getSharedMap().containsKey("selectThreads"))
      selectThreads = (ArrayList<Integer>)SnappyBB.getBB().getSharedMap().get("selectThreads");
    else
      selectThreads = new ArrayList<>();
    if (!selectThreads.contains(testInstance.getMyTid())) {
      selectThreads.add(testInstance.getMyTid());
      SnappyBB.getBB().getSharedMap().put("selectThreads", selectThreads);
    }
    testInstance.releaseDmlLock(BBLock_select);
  }

  protected void getBBLock(hydra.blackboard.SharedLock lock) {
    if (lock == null)
      lock = SnappyBB.getBB().getSharedLock();
    lock.lock();
  }

  protected void releaseDmlLock(hydra.blackboard.SharedLock lock) {
    lock.unlock();
  }


  public static void HydraTask_executeQueries() {
    testInstance.executeSelect();
  }

  public void executeSelect() {
    Connection conn = null;
    int tid = getDMLThread();
    String dmlOp = getOperationForDMLThr(tid);
    String query = "";
    if(dmlOp == "insert" || dmlOp == "delete")
      query = selectStmt[0];
    else
      query = selectStmt[1];
    try {
      conn = getLocatorConnection();
      ResultSet beforeDMLRS = conn.createStatement().executeQuery(query);
      waitForBarrier(2);
      ResultSet afterDMLRS = conn.createStatement().executeQuery(query);
      if (!verifyAtomicity()) {
        throw new TestException("Test failed to get atomic data during DML operations");
      }
    } catch (SQLException se) {

    }
  }

  public String getOperationForDMLThr(int tid){
    return (String)SnappyBB.getBB().getSharedMap().get("op_" + tid);
  }

  public int getDMLThread(){
    int myTid = getMyTid();
    ArrayList<Integer> dmlThreads = (ArrayList<Integer>) SnappyBB.getBB().getSharedMap().get("dmlThreads");
    ArrayList<Integer> selectThreads = (ArrayList<Integer>) SnappyBB.getBB().getSharedMap().get("selectThreads");
    int index = selectThreads.indexOf(myTid);
    return dmlThreads.get(index);
  }

  public static boolean verifyAtomicity() {
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
      switch (DMLOp.getOperation(operation)) {
        case INSERT:
          Log.getLogWriter().info("Performing insert operation...");
          performBulkInsert(conn);
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
      registerOperationInBB(operation);
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing DML Ops. Exception is : " ,se);
    }
  }

  public void registerOperationInBB(String operation){
    SnappyBB.getBB().getSharedMap().put("op_" + getMyTid(),operation);
  }

  public void performBulkInsert(Connection conn) {
    int tid = getMyTid();
    String query = "select max(id) from tab1";
    int maxID = 0;
    try {
      ResultSet rs = conn.createStatement().executeQuery(query);
      while(rs.next()) {
        maxID = rs.getInt(1);
      }
      PreparedStatement ps = conn.prepareStatement(insertStmt[0]);
      for(int i = 0; i<batchSize ; i++) {
        ps.setInt(1, maxID++);
        ps.setString(2,"name" + maxID);
        ps.setInt(3, -1);
        ps.setInt(4, tid);
        ps.addBatch();
      }
      waitForBarrier(2);
      ps.executeBatch();
      Log.getLogWriter().info("Inserted " + batchSize + " rows in the table.");
    } catch( SQLException se) {

    }
  }

  public void performBatchUpdate(Connection conn) {
    String query = updateStmt[0];
    waitForBarrier(2);
    try {
      conn.createStatement().execute(query);
    } catch (SQLException se) {
    }
  }

  public void performBulkDelete(Connection conn) {
    String query = deleteStmt[0];
    waitForBarrier(2);
    try {
      conn.createStatement().execute(query);
    } catch (SQLException se) {
    }
  }

  public void performPutInto(Connection conn) {
    waitForBarrier(2);
  }

  protected void waitForBarrier(int numThreads) {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, "barrier");
    Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier");
    barrier.await();
  }

}
