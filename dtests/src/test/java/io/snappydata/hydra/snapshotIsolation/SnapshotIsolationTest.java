/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
import io.snappydata.hydra.cluster.SnappyBB;
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
  protected static hydra.blackboard.SharedLock lock;

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
    hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
    testUniqueKeys = TestConfig.tab().booleanAt(SQLPrms.testUniqueKeys, true);

    if (testInstance == null) {
      testInstance = new SnapshotIsolationTest();
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.DMLExecuting);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.PauseDerby);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.BlockOps);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.firstThread);
      SnappyBB.getBB().getSharedCounters().setIfLarger(SnappyBB.insertCounter, 1);
      int dmlTableLength = SnapshotIsolationPrms.getDMLTables().length;
      ArrayList<Integer> insertCounters = new ArrayList<>();
      for (int i = 0; i < dmlTableLength; i++) {
        insertCounters.add(1);
      }
      SnappyBB.getBB().getSharedMap().put("insertCounters",insertCounters);
    }
  }

  public static void HydraTask_initializeDMLThreads() {
    testInstance.getLock();
    ArrayList<Integer> dmlthreads;
    if (SnappyBB.getBB().getSharedMap().containsKey("dmlThreads"))
      dmlthreads = (ArrayList<Integer>)SnappyBB.getBB().getSharedMap().get("dmlThreads");
    else
      dmlthreads = new ArrayList<>();
    if (!dmlthreads.contains(testInstance.getMyTid())) {
      dmlthreads.add(testInstance.getMyTid());
      SnappyBB.getBB().getSharedMap().put("dmlThreads", dmlthreads);
    }
    testInstance.releaseLock();
  }

  public static void HydraTask_initializeTablesMetaData() {
    testInstance.saveTableMetaDataToBB();
  }

  public void saveTableMetaDataToBB() {
    try {
      Connection conn = getLocatorConnection();
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
        SnappyBB.getBB().getSharedMap().put("tableMetaData_" + table, sType);
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while saving table metadata to BB. Exception is : " ,
          se);
    } catch (ClassNotFoundException ce) {
      throw new TestException("Got exception while saving table metadata to BB.. Exception is : " ,ce);
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
          Log.getLogWriter().info("Test will perform insert operation.");
          performInsert(conn);
          break;
        case UPDATE:
          Log.getLogWriter().info("Test will perform update operation.");
          performUpdate(conn);
          break;
        case DELETE:
          Log.getLogWriter().info("Test will perform delete operation.");
          performDelete(conn);
          break;
        default: Log.getLogWriter().info("Invalid operation. ");
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
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      ResultSet[] snappyRS = new ResultSet[5];
      ResultSet derbyRS = null;
      List<Struct>[] snappyList = new List[5], derbyList = new List[5];
      if (hasDerbyServer)
        dConn = getDerbyConnection();
      String query = SnapshotIsolationPrms.getSelectStmts();
      for (int i = 0; i < 5; i++) {
        Log.getLogWriter().info("Blocking snappy Ops.");
        getLock();
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.BlockOps);
        releaseLock();
        TestHelper.waitForCounter(SnappyBB.getBB(), "SnappyBB.DMLExecuting", SnappyBB.DMLExecuting,
            0, true, -1, 1000);
        Log.getLogWriter().info("Executing " + query + " on snappy.");
        snappyRS[i] = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
        replayOpsInDerby();
        //run select query in derby
        Log.getLogWriter().info("Executing " + query + " on derby.");
        derbyRS = dConn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on derby.");
        Log.getLogWriter().info("Pausing derby Op.");
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.PauseDerby);
        SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.BlockOps);
        StructTypeImpl sti = ResultSetHelper.getStructType(derbyRS);
        derbyList[i] = ResultSetHelper.asList(derbyRS, sti, true);
        derbyRS.close();
        Thread.sleep(1000);
      }
      for(int i = 0 ; i< 5; i++) {
        StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS[i]);
        snappyList[i] = ResultSetHelper.asList(snappyRS[i], snappySti, false);
        compareResultSets(derbyList[i], snappyList[i]);
        snappyRS[i].close();
      }
      Log.getLogWriter().info("Releasing derby Ops.");
      SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.PauseDerby);
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
      Log.getLogWriter().info("Blocking snappy Ops.");
      getLock();
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.BlockOps);
      releaseLock();
      TestHelper.waitForCounter(SnappyBB.getBB(),"SnappyBB.DMLExecuting", SnappyBB.DMLExecuting ,
          0, true, -1, 1000);
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      ResultSet snappyRS = conn.createStatement().executeQuery(query);
      Log.getLogWriter().info("Executed query on snappy.");
      replayOpsInDerby();
      if(hasDerbyServer)
        dConn = getDerbyConnection();
      //run select query in derby
      Log.getLogWriter().info("Executing " + query + " on derby.");
      ResultSet derbyRS = dConn.createStatement().executeQuery(query);
      Log.getLogWriter().info("Executed query on derby.");
      Log.getLogWriter().info("Pausing derby Op.");
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.PauseDerby);
      SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.BlockOps);
      StructTypeImpl sti = ResultSetHelper.getStructType(derbyRS);
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, sti, true);
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);
        Log.getLogWriter().info("Derby sti is : " + sti.toString());
        Log.getLogWriter().info("Snappy sti is :" + snappySti.toString());
      compareResultSets(derbyList, snappyList);
      Log.getLogWriter().info("Releasing derby Ops.");
      SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.PauseDerby);
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
    int n = new Random().nextInt(dmlTable.length);
    String tableName = dmlTable[n];
    String row = getRowFromCSV(tableName, n);
    row = row + "," + getMyTid();
    Log.getLogWriter().info("Selected row is : " + row);
    PreparedStatement snappyPS, derbyPS;
    String insertStmt = SnapshotIsolationPrms.getInsertStmts()[n];
    TestHelper.waitForCounter(SnappyBB.getBB(), "SnappyBB.BlockOps", SnappyBB.BlockOps,
        0, true, -1, 1000);
    SnappyBB.getBB().getSharedCounters().increment(SnappyBB.DMLExecuting);
    snappyPS = getPreparedStatement(conn, tableName, insertStmt, row);
    Log.getLogWriter().info("Inserting in snappy with statement : " + insertStmt + " with values("
        + row + ")");
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Inserted " + rowCount + " row in snappy.");
    snappyPS.close();
    if (hasDerbyServer) {
      if (SnappyBB.getBB().getSharedCounters().read(SnappyBB.PauseDerby) == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        replayOpsInDerby();
        dConn = getDerbyConnection();
        derbyPS = getPreparedStatement(dConn, tableName, insertStmt, row);
        Log.getLogWriter().info("Inserting in derby with statement : " + insertStmt + " with " +
            "values(" + row + "," + getMyTid() + ")");
        rowCount = derbyPS.executeUpdate();
        Log.getLogWriter().info("Inserted " + rowCount + " row in derby.");
        derbyPS.close();
      } else {
        //need to write operation to BB
        writeOpToBB(tableName,insertStmt,row);
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }
    SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.DMLExecuting);
    Log.getLogWriter().info("Done performing insert operation.");
  }

  public void performUpdate(Connection conn) throws SQLException {
    String updateStmt = SnapshotIsolationPrms.getUpdateStmts();
    if(updateStmt.toUpperCase().contains("WHERE"))
      updateStmt = updateStmt + " AND tid=" + getMyTid();
    else updateStmt = updateStmt + " WHERE tid=" + getMyTid();
    Connection dConn = null;
    TestHelper.waitForCounter(SnappyBB.getBB(), "SnappyBB.BlockOps", SnappyBB.BlockOps,
        0, true, -1, 1000);
    SnappyBB.getBB().getSharedCounters().increment(SnappyBB.DMLExecuting);
    PreparedStatement snappyPS = conn.prepareStatement(updateStmt);
    Log.getLogWriter().info("Update statement is : " + updateStmt);
    //snappyPS.setInt(1,getMyTid());
    Log.getLogWriter().info("Updating in snappy with statement : " + updateStmt);
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Updated " + rowCount + " rows in snappy.");
    if (hasDerbyServer) {
      if (SnappyBB.getBB().getSharedCounters().read(SnappyBB.PauseDerby) == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        replayOpsInDerby();
        dConn = getDerbyConnection();
        PreparedStatement derbyPS = dConn.prepareStatement(updateStmt);
        //derbyPS.setInt(1,getMyTid());
        Log.getLogWriter().info("Updating in derby with statement : " + updateStmt);
        rowCount = derbyPS.executeUpdate();
        Log.getLogWriter().info("Updated " + rowCount + " rows in derby.");
      } else {
        //need to write operation to BB
        writeOpToBB(null,updateStmt,null);
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }
    SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.DMLExecuting);
    Log.getLogWriter().info("Done performing update operation.");
  }

  public void performDelete(Connection conn) throws SQLException {
    String deleteStmt = SnapshotIsolationPrms.getDeleteStmts();
    if(deleteStmt.toUpperCase().contains("WHERE"))
      deleteStmt = deleteStmt + " AND tid=" + getMyTid();
    else deleteStmt = deleteStmt + " WHERE tid=" + getMyTid();
    Connection dConn = null;
    TestHelper.waitForCounter(SnappyBB.getBB(), "SnappyBB.BlockOps", SnappyBB.BlockOps,
        0, true, -1, 1000);
    SnappyBB.getBB().getSharedCounters().increment(SnappyBB.DMLExecuting);
    PreparedStatement snappyPS = conn.prepareStatement(deleteStmt);
    Log.getLogWriter().info("Delete statement is : " + deleteStmt);
    //snappyPS.setInt(1,getMyTid());
    Log.getLogWriter().info("Deleting in snappy with statement : " + deleteStmt);
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Deleted " + rowCount + " rows in snappy.");
    if (hasDerbyServer) {
      if (SnappyBB.getBB().getSharedCounters().read(SnappyBB.PauseDerby) == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        replayOpsInDerby();
        dConn = getDerbyConnection();
        PreparedStatement derbyPS = dConn.prepareStatement(deleteStmt);
        //derbyPS.setInt(1,getMyTid());
        Log.getLogWriter().info("Deleting in derby with statement : " + deleteStmt);
        rowCount = derbyPS.executeUpdate();
        Log.getLogWriter().info("Deleted " + rowCount + " rows  in derby.");
      } else {
        //need to write operation to BB
        writeOpToBB(null,deleteStmt,null);
      }
      if (dConn != null)
        closeDiscConnection(dConn, true);
    }
    SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.DMLExecuting);
    Log.getLogWriter().info("Done performing delete operation.");
  }

  public String getRowFromCSV(String tableName,int rand) {
    String row = null;
    int insertCounter;
    String csvFilePath = SnapshotIsolationPrms.getCsvLocationforLargeData();
    String csvFileName = SnapshotIsolationPrms.getInsertCsvFileNames()[rand];
    getLock();
    List<Integer> counters = (List<Integer>)SnappyBB.getBB().getSharedMap().get("insertCounters");
    insertCounter = counters.get(rand);
    counters.add(rand, insertCounter + 1);
    SnappyBB.getBB().getSharedMap().put("insertCounters", counters);
    releaseLock();
    Log.getLogWriter().info("insert Counter is :" + insertCounter);
    Log.getLogWriter().info("csv path is :" + csvFilePath + File.separator + csvFileName);
    try (Stream<String> lines = Files.lines(Paths.get(csvFilePath + File.separator + csvFileName))) {
      row = lines.skip(insertCounter).findFirst().get();
    } catch (IOException io) {
    }
    //row = "insert into " + tableName + " values (" + row + "," + getMyTid() + ")";
    return row;
  }

  public PreparedStatement getPreparedStatement(Connection conn,String tableName, String stmt,
      String row) {
    PreparedStatement ps = null;
    String[] columnValues = row.split(",");
    try {
      ps = conn.prepareStatement(stmt);
      StructTypeImpl sType = (StructTypeImpl)SnappyBB.getBB().getSharedMap().get
          ("tableMetaData_" + tableName);
      ObjectType[] oTypes = sType.getFieldTypes();
      String[] fieldNames = sType.getFieldNames();
      for (int i = 0; i < oTypes.length; i++) {
        String clazz = oTypes[i].getSimpleClassName();
        String columnValue = columnValues[i];
        switch (clazz) {
          case "String":
            ps.setString(i + 1, columnValue);
            break;
          case "Timestamp":
            Timestamp ts = Timestamp.valueOf(columnValue);
            ps.setTimestamp(i + 1, ts);
            break;
          case "Integer":
            if (columnValue.equalsIgnoreCase("NULL"))
              ps.setNull(i + 1, Types.INTEGER);
            else
              ps.setInt(i + 1, Integer.parseInt(columnValue));
            break;
          case "Double":
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
      if (SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.firstThread) == 1) {
        //remove the results of previous select query from BB
        if (SnappyBB.getBB().getSharedMap().containsKey("firstThreadTid")) {
          int previousFirstTid = (int)SnappyBB.getBB().getSharedMap().get("firstThreadTid");
          SnappyBB.getBB().getSharedMap().remove("thr_" + previousFirstTid);
          SnappyBB.getBB().getSharedCounters().zero(SnappyBB.firstResultsReady);
        }
        //add details for new first thread
        Log.getLogWriter().info("Adding info for firstThread: " + myTid + " to BB");
        SnappyBB.getBB().getSharedMap().put("firstThreadTid", myTid);
        SnappyBB.getBB().getSharedMap().put("query", SnapshotIsolationPrms.getSelectStmts());
      }
      Log.getLogWriter().info("Blocking snappy Ops.");
      getLock();
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.BlockOps);
      releaseLock();
      TestHelper.waitForCounter(SnappyBB.getBB(),"SnappyBB.DMLExecuting", SnappyBB.DMLExecuting ,
          0, true, -1, 1000);
      waitForBarrier(numThreadsPerformingSelect);
      query = (String)SnappyBB.getBB().getSharedMap().get("query");
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      ResultSet snappyRS = conn.createStatement().executeQuery(query);
      Log.getLogWriter().info("Executed query on snappy.");
      //notify to have dml ops started
      SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.BlockOps);
      StructTypeImpl sti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, sti, true);
      int firstTid = (int)SnappyBB.getBB().getSharedMap().get("firstThreadTid");
      if (myTid == firstTid) {
        //write resultSet from first thr to BB
        Log.getLogWriter().info("Adding results for firstThread: " + myTid + " to BB");
        SnappyBB.getBB().getSharedMap().put("thr_" + myTid, snappyList);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.firstResultsReady);
        SnappyBB.getBB().getSharedCounters().zero(SnappyBB.firstThread);
      } else {
        //compare results with first thread results
        TestHelper.waitForCounter(SnappyBB.getBB(), "SnappyBB.firstResultsReady",
            SnappyBB.firstResultsReady, 1, true, -1, 1000);
        Log.getLogWriter().info("Reading results for firstThread: " + firstTid + " from BB");
        List<Struct> list2 = (List<Struct>)SnappyBB.getBB().getSharedMap().get("thr_" + firstTid);
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

  public void performBatchInsert(Connection conn) throws SQLException{
    Statement bstmt = conn.createStatement();
    String insertStmt ;
    for(int i = 0; i< 10; i++) {
      //insertStmt = getInsertStmt();
      //bstmt.addBatch(insertStmt);
    }
    bstmt.executeBatch();
  }

  /* Verify results at the end of the test*/
  public static void HydraTask_verifyResults() {
    testInstance.verifyResults();
  }

  public void verifyResults() {
    StringBuffer mismatchString = new StringBuffer();
    String tableName="";
    try {
      String[] tables = SnapshotIsolationPrms.getTableNames();
      String stmt = "select * from ";
      Connection conn = getLocatorConnection();
      Connection dConn = getDerbyConnection();
      for (String table : tables) {
        tableName = table;
        ResultSet snappyRS = conn.createStatement().executeQuery(stmt + table);
        List<Struct> snappyList = convertToList(snappyRS,false);
        ResultSet derbyRS = dConn.createStatement().executeQuery(stmt + table);
        List<Struct> derbyList = convertToList(derbyRS,true);
        compareResultSets(snappyList,derbyList);
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
      aStr.append("the following " + unexpected.size() + " unexpected elements resultSet: " + listToString(unexpected));
    }

    if (aStr.length() != 0) {
      Log.getLogWriter().info("ResultSet from " +
          first + " is " + listToString(firstResultSet));
      Log.getLogWriter().info("ResultSet from " +
          second + " is " + listToString(secondResultSet));
      Log.getLogWriter().info("ResultSet difference is " + aStr.toString());
      printOpsInBB();
      throw new TestException(aStr.toString());
    }
    if (firstResultSet.size() == secondResultSet.size()) {
      Log.getLogWriter().info("verified that results are correct");
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

  public static void printOpsInBB() {
    List<String> derbyOps = (ArrayList<String>)SnappyBB.getBB().getSharedMap().get("derbyOps");
    if (derbyOps != null) {
      Log.getLogWriter().info("Pending Ops in BB are :");
      for (String op : derbyOps)
        Log.getLogWriter().info(op);
    }
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
    createSchemas(conn);
    Log.getLogWriter().info("done creating schemas on disc.");
    closeDiscConnection(conn,true);
  }

  public static void HydraTask_createSnappySchemas() {
    testInstance.createSnappySchemas();
  }

  protected void createSnappySchemas() {
    try{
    int ddlThread = getMyTid();
    Connection conn = getLocatorConnection();
    Log.getLogWriter().info("creating schemas in gfe.");
    createSchemas(conn);
    Log.getLogWriter().info("done creating schemas in gfe.");
    closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void createSchemas(Connection conn) {
    String[] schemas = SQLPrms.getSchemas();
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < schemas.length; i++) {
        s.execute(schemas[i]);
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Y68")) {
        Log.getLogWriter().info("got schema existing exception if multiple threads" +
            " try to create schema, continuing tests");
      } else
        SQLHelper.handleSQLException(se);
    }
    StringBuffer aStr = new StringBuffer("Created schemas \n");
    for (int i = 0; i < schemas.length; i++) {
      Object o = schemas[i];
      aStr.append(o.toString() + "\n");
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
      Log.getLogWriter().info("dropping tables in gfe.");
      dropTables(conn); //drop table before creating it
      Log.getLogWriter().info("done dropping tables in gfe");
      Log.getLogWriter().info("creating tables in gfe.");
      createTables(conn,false);
      Log.getLogWriter().info("done creating tables in gfe.");
      closeConnection(conn);

    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void createTables(Connection conn, boolean isDerby) {
    //to get create table statements from config file
    String[] createTablesDDL = getCreateTablesStatements(true);
    String[] tableNames = SnapshotIsolationPrms.getTableNames();
    String[] ddlExtn = SnapshotIsolationPrms.getSnappyDDLExtn();
    try {
      Statement s = conn.createStatement();
      if (isDerby) {
        for (int i = 0; i < createTablesDDL.length; i++) {
          Log.getLogWriter().info("about to create table : " + createTablesDDL[i]);
          s.execute(createTablesDDL[i]);
          Log.getLogWriter().info("Created table " + createTablesDDL[i]);
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
    StringBuffer aStr = new StringBuffer("Created tables \n");
    for (int i = 0; i < tableNames.length; i++) {
      aStr.append(tableNames[i] + "\n");
    }
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
        Log.getLogWriter().info("Loading data into " + tableName);
        String csvFilePath = dataLocation + File.separator + csvFileNames[i];
        Log.getLogWriter().info("CSV location is : " + csvFilePath);
        FileInputStream fs = new FileInputStream(csvFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String row = null;
        while ((row = br.readLine()) != null) {
          String rowStmt = insertStmt + row + "," + getMyTid() + ")";
          //Log.getLogWriter().info("Row is : " +  rowStmt);
          conn.createStatement().execute(rowStmt);
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

  public static String[] getCreateTablesStatements(boolean forDerby) {
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

  public synchronized List<List<String>> getDerbyOps() {
    List<List<String>> derbyOps = null;
    if (SnappyBB.getBB().getSharedMap().containsKey("derbyOps")) {
      getLock();
      derbyOps = (ArrayList<List<String>>)SnappyBB.getBB().getSharedMap().get("derbyOps");
      SnappyBB.getBB().getSharedMap().remove("derbyOps");
      releaseLock();
    } else
      Log.getLogWriter().info("No Ops to perform in derby from BB");
    return derbyOps;
  }

  public synchronized void replayOpsInDerby() throws
      SQLException {
    List<List<String>> derbyOps = getDerbyOps();
    //perform operation on derby
    if (derbyOps != null) {
      Connection dConn = getDerbyConnection();
      Log.getLogWriter().info("Performing Ops in derby from BB...");
      PreparedStatement ps;
      for (List<String> operation : derbyOps) {
        String stmt = operation.get(1);
        if(stmt.contains("?")) {
          String tableName = operation.get(0);
          String row = operation.get(2);
           ps = getPreparedStatement(dConn, tableName, stmt, row);
          Log.getLogWriter().info("Performing operation from BB in derby:" + stmt + " with values" +
              "(" + row +")");
        } else {
          ps = dConn.prepareStatement(stmt);
          Log.getLogWriter().info("Performing operation from BB in derby:" + stmt);
        }
        int rowCount = ps.executeUpdate();
        Log.getLogWriter().info("Inserted/Updated/Deleted " + rowCount + " from BB operation in " +
            "derby");
      }
      if(dConn!=null)
        testInstance.closeDiscConnection(dConn, true);
      Log.getLogWriter().info("Performed " + derbyOps.size() + " Ops in derby from BB.");
    } else
      Log.getLogWriter().info("derbyOps is null");
  }

  public synchronized void writeOpToBB(String tableName, String stmt, String row) {
    List<List<String>> derbyOps;
    Log.getLogWriter().info("Adding operation for derby in BB : " + stmt);
    getLock();
    if (SnappyBB.getBB().getSharedMap().containsKey("derbyOps")) {
      derbyOps = (ArrayList<List<String>>)SnappyBB.getBB().getSharedMap().get("derbyOps");
      if(derbyOps==null)
        derbyOps = new ArrayList<>();
    }  else
      derbyOps = new ArrayList<>();
    List l1 = new ArrayList<>();
    l1.add(tableName);
    l1.add(stmt);
    l1.add(row);
    derbyOps.add(l1);
    SnappyBB.getBB().getSharedMap().put("derbyOps", derbyOps);
    releaseLock();
    Log.getLogWriter().info("Added operation for derby in BB : " + stmt);
  }

  protected void getLock() {
    if (lock == null)
      lock = SnappyBB.getBB().getSharedLock();
    lock.lock();
  }

  protected void releaseLock() {
    lock.unlock();
  }
}
