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

package io.snappydata.hydra.snapshotIsolation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.stream.Stream;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import hydra.HydraThreadLocal;
import hydra.HydraVector;
import hydra.Log;
import hydra.MasterController;
import hydra.Prms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.AnyCyclicBarrier;
import io.snappydata.hydra.cluster.SnappyTest;
import sql.ClientDiscDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.sqlutil.GFXDStructImpl;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;


public class SnapshotIsolationTest extends SnappyTest {

  protected static Connection discConn=null;
  public static boolean hasDerbyServer = false;
  public static boolean testUniqueKeys = false;
  public static HydraThreadLocal derbyConnection = new HydraThreadLocal();
  public static HydraThreadLocal resetDerbyConnection = new HydraThreadLocal(); //whether needs to reset the derby connection
  protected static hydra.blackboard.SharedLock counterLock,dmlLock;
  public static boolean hasDuplicateSchemas = false;

  //ENUM for DML Ops
  public enum DMLOp {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

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
      }
      else return null;
    }
  }

  protected static SnapshotIsolationTest testInstance;

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnapshotIsolationTest();
    hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
    testUniqueKeys = TestConfig.tab().booleanAt(SnapshotIsolationPrms.testUniqueKeys, true);

    SnapshotIsolationBB.getBB().getSharedCounters().zero(SnapshotIsolationBB.DMLExecuting);
    SnapshotIsolationBB.getBB().getSharedCounters().zero(SnapshotIsolationBB.PauseDerby);
    SnapshotIsolationBB.getBB().getSharedCounters().zero(SnapshotIsolationBB.BlockOps);
    SnapshotIsolationBB.getBB().getSharedCounters().zero(SnapshotIsolationBB.firstThread);
    int dmlTableLength = SnapshotIsolationPrms.getDMLTables().length;
    ArrayList<Integer> insertCounters = new ArrayList<>();
    for (int i = 0; i < dmlTableLength; i++) {
      insertCounters.add(1);
    }
    if(!SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("insertCounters"))
      SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("insertCounters", insertCounters);
    hasDuplicateSchemas = SnapshotIsolationPrms.hasDuplicateSchemas();
  }

  public static void HydraTask_initializeDMLThreads() {
    if (testInstance == null)
      testInstance = new SnapshotIsolationTest();
    testInstance.getDmlLock();
    ArrayList<Integer> dmlthreads;
    if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlthreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
    else
      dmlthreads = new ArrayList<>();
    if (!dmlthreads.contains(testInstance.getMyTid())) {
      dmlthreads.add(testInstance.getMyTid());
      SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("dmlThreads", dmlthreads);
    }
    testInstance.releaseDmlLock();
  }

  public static void HydraTask_initializeSelectThreads() {
    if (testInstance == null)
      testInstance = new SnapshotIsolationTest();
    testInstance.getDmlLock();
    ArrayList<Integer> selectThreads;
    if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads"))
      selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap()
          .get("selectThreads");
    else
      selectThreads = new ArrayList<>();
    if (!selectThreads.contains(testInstance.getMyTid())) {
      selectThreads.add(testInstance.getMyTid());
      SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("selectThreads", selectThreads);
    }
    testInstance.releaseDmlLock();
  }

  public static void HydraTask_initializeTablesMetaData() {
    try {
      Connection conn = getLocatorConnection();
      testInstance.saveTableMetaDataToBB(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while getting connection. Exception is : ", se);
    }
  }

  public void saveTableMetaDataToBB(Connection conn) {
    try {
      String[] tableNames = SnapshotIsolationPrms.getTableNames();
      for (String table : tableNames) {
        ResultSet rs = conn.createStatement().executeQuery("select * from " + table);
        ResultSetMetaData rsmd = rs.getMetaData();
        int numOfColumns = rsmd.getColumnCount();

        ObjectType[] oTypes = new ObjectType[numOfColumns];
        String[] fieldNames = new String[numOfColumns];
        for (int i = 0; i < numOfColumns; i++) {
          Class<?> clazz = null;
          clazz = Class.forName(rsmd.getColumnClassName(i + 1));
          oTypes[i] = new ObjectTypeImpl(clazz);
          fieldNames[i] = rsmd.getColumnName(i + 1);
        }
        StructTypeImpl sType = new StructTypeImpl(fieldNames, oTypes);
        SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("tableMetaData_" + table, sType);
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while saving table metadata to BB. Exception is : ", se);
    } catch (ClassNotFoundException ce) {
      throw new TestException("Got exception while saving table metadata to BB. Exception is : ", ce);
    }
  }

  /*
   Hydra task to perform DMLOps which can be insert, update, delete
   */
  public static void HydraTask_performDMLOp() {
    testInstance.performDMLOp();
  }

  public void performDMLOp() {
    try {
      Connection conn = getLocatorConnection();
      //perform DML operation which can be insert, update, delete.
      String operation = SnapshotIsolationPrms.getDMLOperations();
      switch (DMLOp.getOperation(operation)) {
        case INSERT:
          Log.getLogWriter().info("Performing insert operation.");
          performInsert(conn);
          break;
        case UPDATE:
          Log.getLogWriter().info("Performing update operation.");
          performUpdate(conn);
          break;
        case DELETE:
          Log.getLogWriter().info("Performing delete operation.");
          performDelete(conn);
          break;
        default: Log.getLogWriter().info("Invalid operation. ");
          throw new TestException("Invalid operation type.");
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing DML Ops. Exception is : " ,se);
    }
  }

  /*
  Hydra task to execute select queries to get multiple snapshot scenario
  */
  public static void HydraTask_testMultipleSnapshot() {
    testInstance.testMultipleSnapshot();
  }

  public void testMultipleSnapshot() {
    try {
      int iterations = 5;
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      ResultSet[] snappyRS = new ResultSet[iterations];
      ResultSet derbyRS = null;
      List<Struct>[] snappyList = new List[iterations], derbyList = new List[iterations];
      String query[] = new String[iterations];
      for (int i = 0; i < iterations; i++) {
        query[i] = SnapshotIsolationPrms.getSelectStmts();
        Log.getLogWriter().info("Blocking operations in snappy and pausing operations in derby.");
        getCounterLock();
        SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.BlockOps);
        SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.PauseDerby);
        releaseCounterLock();
        TestHelper.waitForCounter(SnapshotIsolationBB.getBB(), "SnapshotIsolationBB.DMLExecuting", SnapshotIsolationBB.DMLExecuting,
            0, true, -1, 1000);
        Log.getLogWriter().info("Executing " + query[i] + " on snappy.");
        try {
          snappyRS[i] = conn.createStatement().executeQuery(query[i]);
          Log.getLogWriter().info("Executed query on snappy.");
        } catch(SQLException se){
          if(se.getSQLState().equals("21000")){
            //retry select query with routing
            Log.getLogWriter().info("Got exception while executing select query, retrying with " +
                "executionEngine as spark.");
            String query1 = query[i] +  " --GEMFIREXD-PROPERTIES executionEngine=Spark";
            snappyRS[i] = conn.createStatement().executeQuery(query1);
            Log.getLogWriter().info("Executed query on snappy.");
          }else throw new SQLException(se);
        }

        ArrayList<Integer> dmlThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
            ("dmlThreads");
        for(int n : dmlThreads){
          if(hasDuplicateSchemas)
            replayOpsInDerby(n + "_" + getMyTid());
          else
            replayOpsInDerby(String.valueOf(n));
        }
        if (hasDerbyServer)
          dConn = getDerbyConnection();
        //run select query in derby
        Log.getLogWriter().info("Executing " + query[i] + " on derby.");
        if(hasDuplicateSchemas) {
          String derby_query = query[i].replace("app", "app_" + getMyTid());
          derbyRS = dConn.createStatement().executeQuery(derby_query);
        }else
          derbyRS = dConn.createStatement().executeQuery(query[i]);
        Log.getLogWriter().info("Executed query on derby.");
        SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.BlockOps);
        StructTypeImpl sti = ResultSetHelper.getStructType(derbyRS);
        derbyList[i] = ResultSetHelper.asList(derbyRS, sti, true);
        derbyRS.close();
        Thread.sleep(1000);
      }
      for(int i = 0 ; i < iterations; i++) {
        StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS[i]);
        snappyList[i] = ResultSetHelper.asList(snappyRS[i], snappySti, false);
        Log.getLogWriter().info("Comparing results for query:" + query[i]);
        compareResultSets(derbyList[i], snappyList[i]);
        snappyRS[i].close();
      }
      Log.getLogWriter().info("Releasing operations in derby.");
      SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.PauseDerby);
      if (dConn != null) {
        closeDiscConnection(dConn, true);
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    } catch (InterruptedException ie){
      throw new TestException("Got exception while executing select query.", ie);
    }
  }

  /*
  Hydra task to execute select queries
  */
  public static void HydraTask_executeQueries() {
    testInstance.executeQuery();
  }

  public void executeQuery() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      String query = SnapshotIsolationPrms.getSelectStmts();
      Log.getLogWriter().info("Blocking operations in snappy.");
      getCounterLock();
      SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.BlockOps);
      Log.getLogWriter().info("Pausing operations in derby.");
      SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.PauseDerby);
      releaseCounterLock();
      ResultSet snappyRS;
      TestHelper.waitForCounter(SnapshotIsolationBB.getBB(),"SnapshotIsolationBB.DMLExecuting", SnapshotIsolationBB.DMLExecuting ,
          0, true, -1, 1000);
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      try {
        snappyRS = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
      } catch(SQLException se){
        if(se.getSQLState().equals("21000")){
          //retry select query with routing
          Log.getLogWriter().info("Got exception while executing select query, retrying with " +
              "executionEngine as spark.");
          String query1 = query +  " --GEMFIREXD-PROPERTIES executionEngine=Spark";
          snappyRS = conn.createStatement().executeQuery(query1);
          Log.getLogWriter().info("Executed query on snappy.");
        }else throw new SQLException(se);
      }
      ArrayList<Integer> dmlthreads = null;
      if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads")) {
        dmlthreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
        for (int i : dmlthreads) {
          if(hasDuplicateSchemas)
            replayOpsInDerby(i + "_" + getMyTid());
          else
            replayOpsInDerby(String.valueOf(i));
        }
      }
      if(hasDerbyServer)
        dConn = getDerbyConnection();
      ResultSet derbyRS = null;
      //run select query in derby
      Log.getLogWriter().info("Executing " + query + " on derby.");
      if(hasDuplicateSchemas) {
        String derby_query = query.replace("app", "app_" + getMyTid());
        derbyRS = dConn.createStatement().executeQuery(derby_query);
      }else
        derbyRS = dConn.createStatement().executeQuery(query);
      Log.getLogWriter().info("Executed query on derby.");
      SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.BlockOps);
      StructTypeImpl sti = ResultSetHelper.getStructType(derbyRS);
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, sti, true);
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);
      //Log.getLogWriter().info("Derby sti is : " + sti.toString());
      //Log.getLogWriter().info("Snappy sti is :" + snappySti.toString());
      compareResultSets(derbyList, snappyList);
      Log.getLogWriter().info("Releasing operations for derby.");
      SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.PauseDerby);
      snappyRS.close();
      derbyRS.close();
      if (dConn!=null) {
        closeDiscConnection(dConn, true);
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  public void performInsert(Connection conn) throws SQLException {
    Connection dConn = null;
    String[] dmlTable = SnapshotIsolationPrms.getDMLTables();
    int rand = new Random().nextInt(dmlTable.length);
    String tableName = dmlTable[rand];
    String row = getRowFromCSV(tableName, rand);
    if(testUniqueKeys)
      row = row + "," + getMyTid();
    //Log.getLogWriter().info("Selected row is : " + row);
    PreparedStatement snappyPS, derbyPS = null;
    String insertStmt = SnapshotIsolationPrms.getInsertStmts()[rand];
    TestHelper.waitForCounter(SnapshotIsolationBB.getBB(), "SnapshotIsolationBB.BlockOps", SnapshotIsolationBB.BlockOps,
        0, true, -1, 1000);
    SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.DMLExecuting);
    snappyPS = getPreparedStatement(conn, null, tableName, insertStmt, row);
    Log.getLogWriter().info("Inserting row in snappy with statement : " + insertStmt + " and " +
        "values(" + row + ")");
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Inserted " + rowCount + " row in snappy.");
    snappyPS.close();
    if (hasDerbyServer) {
      if (SnapshotIsolationBB.getBB().getSharedCounters().read(SnapshotIsolationBB.PauseDerby) == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String stmt = insertStmt.replace("app", "app_" + selectTid);
              replayOpsInDerby(getMyTid() + "_" + selectTid);
              dConn = getDerbyConnection();
              derbyPS = getPreparedStatement(dConn,null, tableName, stmt, row);
              Log.getLogWriter().info("Inserting row in derby app_" + selectTid + " with " +
                  "statement : " + insertStmt + " and values(" + row + ")");
              rowCount = derbyPS.executeUpdate();
              Log.getLogWriter().info("Inserted " + rowCount + " row in derby app_" + selectTid);
            }
          }
        } else {
          replayOpsInDerby(String.valueOf(getMyTid()));
          dConn = getDerbyConnection();
          derbyPS = getPreparedStatement(dConn,null, tableName, insertStmt, row);
          Log.getLogWriter().info("Inserting row in derby with statement : " + insertStmt +
              " and values(" + row + ")");
          rowCount = derbyPS.executeUpdate();
          Log.getLogWriter().info("Inserted " + rowCount + " row in derby.");
        }
        derbyPS.close();
      } else {
        //need to write operation to BB
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String stmt = insertStmt.replace("app", "app_" + selectTid);
              writeOpToBB(tableName, stmt, row, getMyTid() + "_" + selectTid);
            }
          }
        } else
          writeOpToBB(tableName, insertStmt, row, String.valueOf(getMyTid()));
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }
    SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.DMLExecuting);
    Log.getLogWriter().info("Done performing insert operation.");
  }

  public void performUpdate(Connection conn) throws SQLException {
    String updateStmt = SnapshotIsolationPrms.getUpdateStmts();
    if(testUniqueKeys) {
      if (updateStmt.toUpperCase().contains("WHERE"))
        updateStmt = updateStmt + " AND tid=" + getMyTid();
      else updateStmt = updateStmt + " WHERE tid=" + getMyTid();
    }
    Connection dConn = null;
    TestHelper.waitForCounter(SnapshotIsolationBB.getBB(), "SnapshotIsolationBB.BlockOps", SnapshotIsolationBB.BlockOps,
        0, true, -1, 1000);
    SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.DMLExecuting);
    PreparedStatement snappyPS = conn.prepareStatement(updateStmt);
    //Log.getLogWriter().info("Update statement is : " + updateStmt);
    //snappyPS.setInt(1,getMyTid());
    Log.getLogWriter().info("Updating in snappy with statement : " + updateStmt);
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Updated " + rowCount + " rows in snappy.");
    if (hasDerbyServer) {
      if (SnapshotIsolationBB.getBB().getSharedCounters().read(SnapshotIsolationBB.PauseDerby) == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String stmt = updateStmt.replace("app", "app_" + selectTid);
              replayOpsInDerby(getMyTid() + "_" + selectTid);
              dConn = getDerbyConnection();
              PreparedStatement derbyPS = dConn.prepareStatement(stmt);
              //derbyPS.setInt(1,getMyTid());
              Log.getLogWriter().info("Updating in derby with statement : " + stmt);
              rowCount = derbyPS.executeUpdate();
              Log.getLogWriter().info("Updated " + rowCount + " rows in derby.");
            }
          }
        } else {
          replayOpsInDerby(String.valueOf(getMyTid()));
          dConn = getDerbyConnection();
          PreparedStatement derbyPS = dConn.prepareStatement(updateStmt);
          //derbyPS.setInt(1,getMyTid());
          Log.getLogWriter().info("Updating in derby with statement : " + updateStmt);
          rowCount = derbyPS.executeUpdate();
          Log.getLogWriter().info("Updated " + rowCount + " rows in derby.");
        }
      } else {
        //need to write operation to BB
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String stmt = updateStmt.replace("app", "app_" + selectTid);
              writeOpToBB(null, stmt, null, getMyTid() + "_" + selectTid);
            }
          }
        }else
          writeOpToBB(null, updateStmt, null, String.valueOf(getMyTid()));
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }
    SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.DMLExecuting);
    Log.getLogWriter().info("Done performing update operation.");
  }

  public void performDelete(Connection conn) throws SQLException {
    String deleteStmt = SnapshotIsolationPrms.getDeleteStmts();
    if(testUniqueKeys) {
      if (deleteStmt.toUpperCase().contains("WHERE"))
        deleteStmt = deleteStmt + " AND tid=" + getMyTid();
      else deleteStmt = deleteStmt + " WHERE tid=" + getMyTid();
    }
    Connection dConn = null;
    TestHelper.waitForCounter(SnapshotIsolationBB.getBB(), "SnapshotIsolationBB.BlockOps", SnapshotIsolationBB.BlockOps,
        0, true, -1, 1000);
    SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.DMLExecuting);
    PreparedStatement snappyPS = conn.prepareStatement(deleteStmt);
    //Log.getLogWriter().info("Delete statement is : " + deleteStmt);
    //snappyPS.setInt(1,getMyTid());
    Log.getLogWriter().info("Deleting in snappy with statement : " + deleteStmt);
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Deleted " + rowCount + " rows in snappy.");
    if (hasDerbyServer) {
      if (SnapshotIsolationBB.getBB().getSharedCounters().read(SnapshotIsolationBB.PauseDerby) == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String stmt = deleteStmt.replace("app", "app_" + selectTid);
              replayOpsInDerby(getMyTid() + "_" + selectTid);
              dConn = getDerbyConnection();
              PreparedStatement derbyPS = dConn.prepareStatement(stmt);
              //derbyPS.setInt(1,getMyTid());
              Log.getLogWriter().info("Deleting in derby with statement : " + deleteStmt);
              rowCount = derbyPS.executeUpdate();
              Log.getLogWriter().info("Deleted " + rowCount + " rows  in derby.");
            }
          }
        } else {
          replayOpsInDerby(String.valueOf(getMyTid()));
          dConn = getDerbyConnection();
          PreparedStatement derbyPS = dConn.prepareStatement(deleteStmt);
          //derbyPS.setInt(1,getMyTid());
          Log.getLogWriter().info("Deleting in derby with statement : " + deleteStmt);
          rowCount = derbyPS.executeUpdate();
          Log.getLogWriter().info("Deleted " + rowCount + " rows  in derby.");
        }
      } else {
        //need to write operation to BB
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String stmt = deleteStmt.replace("app", "app_"+selectTid);
              writeOpToBB(null, stmt, null, getMyTid() + "_" + selectTid);
            }
          }
        }else
          writeOpToBB(null, deleteStmt, null, String.valueOf(getMyTid()));
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }
    SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.DMLExecuting);
    Log.getLogWriter().info("Done performing delete operation.");
  }

  public static void HydraTask_performBatchInsert() {
    testInstance.performBatchInsert();
  }

  public void performBatchInsert() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      int batchSize = 100;
      String[] dmlTable = SnapshotIsolationPrms.getDMLTables();
      int n = new Random().nextInt(dmlTable.length);
      String tableName = dmlTable[n];
      PreparedStatement snappyPS = null, derbyPS = null;
      String insertStmt = SnapshotIsolationPrms.getInsertStmts()[n];
      if (hasDerbyServer)
        dConn = getDerbyConnection();
      Log.getLogWriter().info("Performing batch insert on table " + tableName + " with batch size: " +
          batchSize);
      for (int i = 0; i < batchSize; i++) {
        String row = getRowFromCSV(tableName, n);
        if (testUniqueKeys)
          row = row + "," + getMyTid();
        //Log.getLogWriter().info("Selected row is : " + row);
        snappyPS = getPreparedStatement(conn, snappyPS, tableName, insertStmt, row);
        snappyPS.addBatch();
        if (hasDerbyServer) {
          derbyPS = getPreparedStatement(dConn, derbyPS, tableName, insertStmt, row);
          derbyPS.addBatch();
        }
      }
      TestHelper.waitForCounter(SnapshotIsolationBB.getBB(), "SnapshotIsolationBB.BlockOps", SnapshotIsolationBB.BlockOps,
          0, true, -1, 1000);
      SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.DMLExecuting);
      Log.getLogWriter().info("Executing batch insert in snappy");
      int updateCnt[] =  snappyPS.executeBatch();
      Log.getLogWriter().info("Inserted " + updateCnt.length + " rows.");
      if (hasDerbyServer) {
        Log.getLogWriter().info("Executing batch insert in derby");
        derbyPS.executeBatch();
        Log.getLogWriter().info("Inserted " + updateCnt.length + " rows.");
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }catch(SQLException se){
      throw new TestException("Got exception while performing batch insert.",se);
    }
  }

  public String getRowFromCSV(String tableName,int randTable) {
    String row = null;
    int insertCounter;
    String csvFilePath = SnapshotIsolationPrms.getCsvLocationforLargeData();
    String csvFileName = SnapshotIsolationPrms.getInsertCsvFileNames()[randTable];
    getDmlLock();
    List<Integer> counters = (List<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("insertCounters");
    insertCounter = counters.get(randTable);
    counters.set(randTable, insertCounter + 1);
    SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("insertCounters", counters);
    releaseDmlLock();
    Log.getLogWriter().info("insert Counter is :" + insertCounter + " for csv " + csvFilePath + File.separator + csvFileName);
    try (Stream<String> lines = Files.lines(Paths.get(csvFilePath + File.separator + csvFileName))) {
      row = lines.skip(insertCounter).findFirst().get();
    } catch (IOException io) {
      throw new TestException("File not found at specified location.");
    }
    return row;
  }

  public PreparedStatement getPreparedStatement(Connection conn,PreparedStatement ps, String
      tableName, String stmt, String row) {
    String[] columnValues = row.split(",");
    try {
      if(ps==null)
        ps = conn.prepareStatement(stmt);
      StructTypeImpl sType = (StructTypeImpl)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
          ("tableMetaData_" + tableName);
      ObjectType[] oTypes = sType.getFieldTypes();
      String[] fieldNames = sType.getFieldNames();
      for (int i = 0; i < oTypes.length; i++) {
        String clazz = oTypes[i].getSimpleClassName();
        String columnValue = columnValues[i];
        switch (clazz) {
          case "String":
            if (columnValue.equalsIgnoreCase("NULL"))
              ps.setNull(i + 1, Types.VARCHAR);
            else
              ps.setString(i + 1, columnValue);
            break;
          case "Timestamp":
            if (columnValue.equalsIgnoreCase("NULL"))
              ps.setNull(i + 1, Types.TIMESTAMP);
            else {
              Timestamp ts = Timestamp.valueOf(columnValue);
              ps.setTimestamp(i + 1, ts);
            }
            break;
          case "Integer":
            if (columnValue.equalsIgnoreCase("NULL"))
              ps.setNull(i + 1, Types.INTEGER);
            else
              ps.setInt(i + 1, Integer.parseInt(columnValue));
            break;
          case "Double":
            if (columnValue.equalsIgnoreCase("NULL"))
              ps.setNull(i + 1, Types.DOUBLE);
            else
              ps.setDouble(i + 1, Double.parseDouble(columnValue));
            break;

        }
      }
    } catch (SQLException se) {
      throw new TestException("Exception while creating PreparedStatement.", se);
    }
    return ps;
  }

  /*
  Hydra task to execute multiple select queries
  */
  public static void HydraTask_multipleExecuteQueries() {
    testInstance.multipleSelectQuery();
  }

  public void multipleSelectQuery() {
    try {
      Connection conn = getLocatorConnection();
      String query = "";
      int myTid = RemoteTestModule.getCurrentThread().getThreadId();
      //total number of threads executing select query
      int numThreadsPerformingSelect = RemoteTestModule.getCurrentThread().getCurrentTask()
          .getTotalThreads();
      //for first thread in the round, assign as first thread.
      if (SnapshotIsolationBB.getBB().getSharedCounters().incrementAndRead(SnapshotIsolationBB.firstThread) == 1) {
        //remove the results of previous select query from BB
        if (SnapshotIsolationBB.getBB().getSharedMap().containsKey("firstThreadTid")) {
          int previousFirstTid = (int)SnapshotIsolationBB.getBB().getSharedMap().get("firstThreadTid");
          SnapshotIsolationBB.getBB().getSharedMap().remove("thr_" + previousFirstTid);
          SnapshotIsolationBB.getBB().getSharedCounters().zero(SnapshotIsolationBB.firstResultsReady);
        }
        //add details for new first thread
        Log.getLogWriter().info("Adding info for firstThread: " + myTid + " to BB");
        SnapshotIsolationBB.getBB().getSharedMap().put("firstThreadTid", myTid);
        SnapshotIsolationBB.getBB().getSharedMap().put("query", SnapshotIsolationPrms.getSelectStmts());
      }
      Log.getLogWriter().info("Blocking operations in snappy.");
      SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.BlockOps);
      TestHelper.waitForCounter(SnapshotIsolationBB.getBB(),"SnapshotIsolationBB.DMLExecuting", SnapshotIsolationBB.DMLExecuting ,
          0, true, -1, 1000);
      waitForBarrier(numThreadsPerformingSelect);
      query = (String)SnapshotIsolationBB.getBB().getSharedMap().get("query");
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      ResultSet snappyRS;
      try {
        snappyRS = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
      } catch(SQLException se){
        if(se.getSQLState().equals("21000")){
          //retry select query with routing
          Log.getLogWriter().info("Got exception while executing select query, retrying with " +
              "executionEngine as spark.");
          String query1 = query +  " --GEMFIREXD-PROPERTIES executionEngine=Spark";
          snappyRS = conn.createStatement().executeQuery(query1);
          Log.getLogWriter().info("Executed query on snappy.");
        }else throw new SQLException(se);
      }
      //notify to have dml ops started
      SnapshotIsolationBB.getBB().getSharedCounters().decrement(SnapshotIsolationBB.BlockOps);
      StructTypeImpl sti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, sti, true);
      int firstTid = (int)SnapshotIsolationBB.getBB().getSharedMap().get("firstThreadTid");
      if (myTid == firstTid) {
        //write resultSet from first thr to BB
        Log.getLogWriter().info("Adding results for firstThread: " + myTid + " to BB");
        SnapshotIsolationBB.getBB().getSharedMap().put("thr_" + myTid, snappyList);
        SnapshotIsolationBB.getBB().getSharedCounters().increment(SnapshotIsolationBB.firstResultsReady);
        SnapshotIsolationBB.getBB().getSharedCounters().zero(SnapshotIsolationBB.firstThread);
      } else {
        //compare results with first thread results
        TestHelper.waitForCounter(SnapshotIsolationBB.getBB(), "SnapshotIsolationBB.firstResultsReady",
            SnapshotIsolationBB.firstResultsReady, 1, true, -1, 1000);
        Log.getLogWriter().info("Reading results for firstThread: " + firstTid + " from BB");
        List<Struct> list2 = (List<Struct>)SnapshotIsolationBB.getBB().getSharedMap().get("thr_" + firstTid);
        compareResultSets(snappyList, list2, "thr_" + myTid, "firstThread");
      }
      snappyRS.close();
      closeConnection(conn);
      //synchronize end of task.
      waitForBarrier(numThreadsPerformingSelect);
      Log.getLogWriter().info("Finished waiting for barrier.");
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void waitForBarrier(int numThreads) {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, "barrier");
    Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier");
    barrier.await();
  }

  /* Verify results at the end of the test*/
  public static void HydraTask_verifyResults() {
    testInstance.verifyResults();
  }

  public void verifyResults() {
    try {
      if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads")) {
        ArrayList<Integer> dmlthreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
        for (int i : dmlthreads) {
          if(hasDuplicateSchemas)
            replayOpsInDerby(i + "_" + getMyTid());
          else
            replayOpsInDerby(String.valueOf(i));
        }
      }
    }catch(SQLException se){
      throw new TestException("Got Exception while replaying ops in derby.",se);
    }

    StringBuffer mismatchString = new StringBuffer();
    String tableName="";
    ResultSet derbyRS = null;
    try {
      String[] tables = SnapshotIsolationPrms.getTableNames();
      String stmt = "select * from ";
      Connection conn = getLocatorConnection();
      Connection dConn = getDerbyConnection();
      for (String table : tables) {
        tableName = table;
        String selectStmt = stmt + table;
        Log.getLogWriter().info("Verifying results for " +  table + " using " + selectStmt);
        ResultSet snappyRS = conn.createStatement().executeQuery(stmt + table);
        List<Struct> snappyList = convertToList(snappyRS,false);
        if (hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads")) {
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String tab = table;
              tab = tab.replace("app", "app_" + selectTid);
              derbyRS = dConn.createStatement().executeQuery(stmt + tab);
              List<Struct> derbyList = convertToList(derbyRS, true);
              compareResultSets(derbyList, snappyList);
            }
          }
        } else {
          derbyRS = dConn.createStatement().executeQuery(stmt + table);
          List<Struct> derbyList = convertToList(derbyRS, true);
          compareResultSets(derbyList, snappyList);
        }
      }
    }catch(SQLException se){
      throw new TestException("Got Exception while verifying the table data.",se);
    }catch(TestException te){
      mismatchString = mismatchString.append("Result mismatch in " + tableName + " :\n");
      mismatchString = mismatchString.append(te.getMessage()).append("\n");
    }
    if(mismatchString.length()>0)
      throw new TestException(mismatchString.toString());
  }

  public List<Struct> convertToList(ResultSet rs, boolean isDerby){
    StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> rsList = ResultSetHelper.asList(rs, sti, isDerby);
    return rsList;
  }

  public static void compareResultSets(List<Struct> derbyResultSet,
      List<Struct> snappyResultSet) {
    compareResultSets(derbyResultSet, snappyResultSet, "derby", "snappy");
  }

  public static void compareResultSets(List<Struct> firstResultSet,
      List<Struct> secondResultSet, String first, String second) {
    Log.getLogWriter().info("size of resultSet from " + first + " is " + firstResultSet.size());
    //Log.getLogWriter().info("Result from " + first + " is :" + listToString(firstResultSet));
    Log.getLogWriter().info("size of resultSet from " + second + " is " + secondResultSet.size());
    //Log.getLogWriter().info("Result from " + second + " is :" + listToString(secondResultSet));

    List<Struct> secondResultSetCopy = new ArrayList<Struct>(secondResultSet);

    StringBuffer aStr = new StringBuffer();
    for (int i = 0; i < firstResultSet.size(); i++) {
      secondResultSetCopy.remove(firstResultSet.get(i));
    }
    List<Struct> unexpected = secondResultSetCopy;
    List<Struct> missing = null;

    if (firstResultSet.size() != secondResultSet.size() || unexpected.size() > 0) {
      List<Struct> firstResultSetCopy = new ArrayList<Struct>(firstResultSet);
      for (int i = 0; i < secondResultSet.size(); i++) {
        firstResultSetCopy.remove(secondResultSet.get(i));
      }
      missing = firstResultSetCopy;

      if (missing.size() > 0) {
        aStr.append("the following " + missing.size() + " elements were missing from "
            + second + " resultSet: " + listToString(missing));
      }
    }
    if (unexpected.size() > 0) {
      aStr.append("the following " + unexpected.size() + " unexpected elements in " + second
              + " resultSet: " + listToString(unexpected));
    }

    if (aStr.length() != 0) {
      Log.getLogWriter().info("ResultSet from " + first + " is " + listToString(firstResultSet));
      Log.getLogWriter().info("ResultSet from " + second + " is " + listToString(secondResultSet));
      Log.getLogWriter().info("ResultSet difference is " + aStr.toString());
      printOpsInBB();
      throw new TestException(aStr.toString());
    }
    if (firstResultSet.size() == secondResultSet.size()) {
      Log.getLogWriter().info("Verified that results are correct.");
    }
    else if (firstResultSet.size() < secondResultSet.size()) {
      throw new TestException("There are more data in " + second + " ResultSet");
    }
    else {
      throw new TestException("There are fewer data in " + second + " ResultSet");
    }
  }

  public static String listToString(List<Struct> aList) {
    if (aList == null) {
      throw new TestException ("test issue, need to check in the test and not pass in null list here");
    }
    StringBuffer aStr = new StringBuffer();
    aStr.append("The size of list is " + (aList == null ? "0" : aList.size()) + "\n");

    for (int i = 0; i < aList.size(); i++) {
      Object aStruct = aList.get(i);
      if (aStruct instanceof com.gemstone.gemfire.cache.query.Struct) {
        GFXDStructImpl si = (GFXDStructImpl)(aStruct);
        aStr.append(si.toString());
      }
      aStr.append("\n");
    }
    return aStr.toString();
  }

   /*
   Methods For derby setup - create and start derby instance, schema and tables in derby. Stop
   derby instance.
   */
  public static synchronized void HydraTask_createDerbyDB() {
    testInstance.createDerbyDB();
  }

  protected void createDerbyDB() {
    if (hasDerbyServer && discConn == null) {
      while (true) {
        try {
          discConn = ClientDiscDBManager.getConnection();
          break;
        } catch (SQLException se) {
          Log.getLogWriter().info("Not able to connect to Derby server yet, Derby server may not be ready.");
          SQLHelper.printSQLException(se);
          int sleepMS = 10000;
          MasterController.sleepForMs(sleepMS); //sleep 10 sec to wait for Derby server to be ready.
        }
      }
    }
  }

  public static void HydraTask_createDerbySchemas() {
    testInstance.createDerbySchema();
  }

  protected void createDerbySchema() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("manageDerbyServer is not set to true.");
      return;
    }
    Connection conn = getDerbyConnection();
    Log.getLogWriter().info("creating schemas on disc.");
    createSchemas(conn,true);
    Log.getLogWriter().info("done creating schemas on disc.");
    closeDiscConnection(conn,true);
  }

  public static void HydraTask_createSnappySchemas() {
    testInstance.createSnappySchemas();
  }

  protected void createSnappySchemas() {
    try{
    Connection conn = getLocatorConnection();
    Log.getLogWriter().info("creating schemas in snappy.");
    createSchemas(conn,false);
    Log.getLogWriter().info("done creating schemas in snappy.");
    closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while creating schemas.", se);
    }
  }

  protected void createSchemas(Connection conn,Boolean isDerby) {
    String[] schemas = SQLPrms.getSchemas();
    StringBuffer aStr = new StringBuffer("Created schemas \n");
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < schemas.length; i++) {
        if (isDerby && hasDuplicateSchemas) {
          ArrayList<Integer> selectThreads = null;
          if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads"))
            selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
          for (int selectTid : selectThreads) {
            String schema = schemas[i].trim() + "_" + selectTid;
            s.execute(schema);
            aStr.append(schema + "\n");
          }
        } else {
          s.execute(schemas[i]);
          aStr.append(schemas[i] + "\n");
        }
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Y68") || se.getSQLState().equals("42000")) {
        Log.getLogWriter().info("got schema existing exception if multiple threads" +
            " try to create schema, continuing tests");
      } else
        SQLHelper.handleSQLException(se);
    }

    Log.getLogWriter().info(aStr.toString());
  }

  /**
   * To create tables in derby
   */

  public static synchronized void HydraTask_createDerbyTables(){
    testInstance.createDerbyTables();
  }

  protected void createDerbyTables() {
    if (!hasDerbyServer) {
      return;
    }
    Connection conn = getDerbyConnection();
    log().info("Creating tables in derby db.");
    createTables(conn,true);
    //loadDerbyTables(conn);
    log().info("Done creating tables in derby db.");
    closeDiscConnection(conn,true);
  }

  public static synchronized void HydraTask_createSnappyTables(){
    testInstance.createSnappyTables();
  }

  protected void createSnappyTables() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("dropping tables in snappy.");
      dropTables(conn); //drop table before creating it
      Log.getLogWriter().info("done dropping tables in snappy");
      Log.getLogWriter().info("creating tables in snappy.");
      createTables(conn,false);
      Log.getLogWriter().info("done creating tables in snappy.");
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while creating tables in snappy.", se);
    }
  }

  protected void createTables(Connection conn, boolean isDerby) {
    //to get create table statements from config file
    String[] createTablesDDL = getCreateTablesStatements();
    String[] ddlExtn = SnapshotIsolationPrms.getSnappyDDLExtn();
    StringBuffer aStr = new StringBuffer("Created tables \n");
    String[] tableNames = SnapshotIsolationPrms.getTableNames();
    try {
      Statement s = conn.createStatement();
      if (isDerby) {
        for (int i = 0; i < createTablesDDL.length; i++) {
          String createDDL = createTablesDDL[i];
          if(hasDuplicateSchemas) {
            ArrayList<Integer> selectThreads = null;
            if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads"))
              selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              createDDL = createTablesDDL[i];
              createDDL = createDDL.replace("app", "app_" + selectTid);
              Log.getLogWriter().info("about to create table : " + createDDL);
              s.execute(createDDL);
              Log.getLogWriter().info("Created table " + createDDL);
            }
          }else {
            Log.getLogWriter().info("about to create table : " + createDDL);
            s.execute(createDDL);
            Log.getLogWriter().info("Created table " + createDDL);
          }
          aStr.append(tableNames[i]);
        }
      } else {
        for (int i = 0; i < createTablesDDL.length; i++) {
          String createDDL = createTablesDDL[i] + ddlExtn[i];
          Log.getLogWriter().info("about to create table : " + createDDL);
          s.execute(createDDL);
          Log.getLogWriter().info("Created table " + createDDL);
        }
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create tables\n"
          + TestHelper.getStackTrace(se));
    }
    if(hasDuplicateSchemas) aStr.append(" with duplicate schemas in derby.");
    Log.getLogWriter().info(aStr.toString());
  }

  protected void dropTables(Connection conn) {
    String sql = null;
    String[] tables = SnapshotIsolationPrms.getTableNames();
    sql = "drop table if exists ";
    try {
      for (String table : tables) {
        Statement s = conn.createStatement();
        s.execute(sql + table);
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while dropping table.", se);
    }
  }

  public static synchronized void HydraTask_populateTables(){
    testInstance.populateTables();
  }

  protected void populateTables() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      if (hasDerbyServer)
        dConn = getDerbyConnection();
      String[] tableNames = SnapshotIsolationPrms.getTableNames();
      String[] csvFileNames = SnapshotIsolationPrms.getCSVFileNames();
      String dataLocation = SnapshotIsolationPrms.getDataLocations();
      for (int i = 0; i < tableNames.length; i++) {
        String tableName = tableNames[i].toUpperCase();
        String insertStmt = "insert into " + tableName + " values (";
        String csvFilePath = dataLocation + File.separator + csvFileNames[i];
        Log.getLogWriter().info("Loading data into " + tableName + " from location: " + csvFilePath);
        FileInputStream fs = new FileInputStream(csvFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String row = null;
        ArrayList<Integer> dmlthreads = null;
        int tid;
        if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
          dmlthreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
        while ((row = br.readLine()) != null) {
          if(dmlthreads==null)
            tid = getMyTid();
          else
            tid = dmlthreads.get(new Random().nextInt(dmlthreads.size()));
          String rowStmt = insertStmt + row + "," + tid + ")";
          //Log.getLogWriter().info("Row is : " +  rowStmt);
          conn.createStatement().execute(rowStmt);
          if(hasDuplicateSchemas) {
            ArrayList<Integer> selectThreads = null;
            if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("selectThreads"))
              selectThreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("selectThreads");
            for (int selectTid : selectThreads) {
              String insert = rowStmt;
              insert = insert.replace("APP", "APP_" + selectTid);
              dConn.createStatement().execute(insert);
            }
          }
          else
            dConn.createStatement().execute(rowStmt);
        }
        Log.getLogWriter().info("Done loading data into table " + tableName );
      }
      conn.close();
      closeDiscConnection(dConn,true);
    } catch (IOException ie) {
      throw new TestException("Got exception while populating table.", ie);
    } catch (SQLException se) {
      throw new TestException("Got exception while populating table.", se);
    }

  }

  public static String[] getCreateTablesStatements() {
    Long key = SQLPrms.createTablesStatements;
    Vector statements = TestConfig.tab().vecAt(key, new HydraVector());
    String[] strArr = new String[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      strArr[i] = (String)statements.elementAt(i);
    }
    return strArr;
  }

  public static void HydraTask_shutDownDerbyDB() {
    testInstance.shutDownDiscDB();
  }

  protected void shutDownDiscDB() {
    if (hasDerbyServer) {
      ClientDiscDBManager.shutDownDB();
    }
  }

  protected Connection getDerbyConnection() {
    Connection conn = (Connection)derbyConnection.get();
    try {
      if (conn == null || (Boolean)resetDerbyConnection.get() || conn.isClosed()) {
        Log.getLogWriter().info("derbyConnection is not set yet");
        try {
          conn = ClientDiscDBManager.getConnection();
        } catch (SQLException e) {
          SQLHelper.printSQLException(e);
          throw new TestException("Not able to get Derby Connection:\n " + TestHelper.getStackTrace(e));
        }
        derbyConnection.set(conn);
        resetDerbyConnection.set(false);
      }
    } catch (Exception e) {
      throw new TestException("Exception while getting derby connection " + " : " + e.getMessage());
    }
    return conn;
  }

  protected void closeDiscConnection(Connection conn, boolean end) {
    //close the connection at end of the test
    if (end) {
      try {
        conn.commit();
        conn.close();
        Log.getLogWriter().info("closing the connection");
      } catch (SQLException e) {
        SQLHelper.printSQLException(e);
        throw new TestException ("Not able to release the connection " + TestHelper.getStackTrace(e));
      }
    }
  }

  public synchronized List<List<String>> getDerbyOps(String tid) {
    List<List<String>> derbyOps = null;
    if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("derbyOps_" + tid)) {
      getDmlLock();
      derbyOps = (ArrayList<List<String>>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("derbyOps_" + tid);
      SnapshotIsolationDMLOpsBB.getBB().getSharedMap().remove("derbyOps_" + tid);
      releaseDmlLock();
    } else
      Log.getLogWriter().info("No operations to be performed from BB in derby." + tid);
    return derbyOps;
  }

  public synchronized void replayOpsInDerby(String tid) throws SQLException {
    List<List<String>> derbyOps = getDerbyOps(tid);
    //perform operation on derby
    if (derbyOps != null) {
      Connection dConn = getDerbyConnection();
      Log.getLogWriter().info("In replay operations from BB in derby :" + tid);
      PreparedStatement ps;
      for (List<String> operation : derbyOps) {
        String stmt = operation.get(1);
        if(stmt.contains("insert ")) {
          String tableName = operation.get(0);
          String row = operation.get(2);
           ps = getPreparedStatement(dConn,null, tableName, stmt, row);
          Log.getLogWriter().info("Performing BB operation:" + stmt + " with values(" + row +")");
        } else {
          ps = dConn.prepareStatement(stmt);
          Log.getLogWriter().info("Performing BB operation:" + stmt);
        }
        int rowCount = ps.executeUpdate();
        Log.getLogWriter().info("Inserted/Updated/Deleted " + rowCount + " rows in derby");
      }
      if(dConn!=null)
        testInstance.closeDiscConnection(dConn, true);
      Log.getLogWriter().info("Performed " + derbyOps.size() + " operations in derby from BB " +
          tid + ".");
    } else
      Log.getLogWriter().info("derbyOps " + tid + " is null");
  }

  public synchronized void writeOpToBB(String tableName, String stmt, String row,String tid) {
    List<List<String>> derbyOps;
    Log.getLogWriter().info("Adding operation for derby in BB " + tid + " : " + stmt);
    List l1 = new ArrayList<>();
    l1.add(tableName);
    l1.add(stmt);
    l1.add(row);
    getDmlLock();
    if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("derbyOps_" + tid)) {
      derbyOps = (ArrayList<List<String>>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("derbyOps_" + tid);
      if (derbyOps == null)
        derbyOps = new ArrayList<>();
    } else
      derbyOps = new ArrayList<>();
    derbyOps.add(l1);
    SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("derbyOps_" + tid, derbyOps);
    releaseDmlLock();
    Log.getLogWriter().info("Added operation for derby in BB " + tid + " : " + stmt);
  }

  public static void printOpsInBB() {
    ArrayList<Integer> dmlthreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
    if(dmlthreads!=null) {
      for (int tid : dmlthreads) {
        List<List<String>> derbyOps = (ArrayList<List<String>>) SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
            ("derbyOps_" + tid);
        if (derbyOps != null) {
          Log.getLogWriter().info("Pending Ops in BB are :");
          for (List<String> operation : derbyOps) {
            String stmt = operation.get(1);
            if (stmt.contains("insert ")) {
              String row = operation.get(2);
              Log.getLogWriter().info(stmt + " with values" + "(" + row + ")");
            } else
              Log.getLogWriter().info(stmt);
          }
        }
      }
    }
  }

  protected void getCounterLock() {
    if (counterLock == null)
      counterLock = SnapshotIsolationBB.getBB().getSharedLock();
    counterLock.lock();
  }

  protected void releaseCounterLock() {
    counterLock.unlock();
  }

  protected void getDmlLock() {
    if (dmlLock == null)
      dmlLock = SnapshotIsolationDMLOpsBB.getBB().getSharedLock();
    dmlLock.lock();
  }

  protected void releaseDmlLock() {
    dmlLock.unlock();
  }

}
