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

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.HydraThreadLocal;
import hydra.HydraVector;
import hydra.Log;
import hydra.MasterController;
import hydra.Prms;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import sql.ClientDiscDBManager;
import sql.DiscDBManager;
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
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.ReadyToBegin);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.PauseDerby);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.BlockOps);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.selectLeadThread);
      SnappyBB.getBB().getSharedCounters().setIfLarger(SnappyBB.insertCounter, 5000);
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
      Connection dConn = null;
      //perform DML operation which can be insert, update, delete.
      String operation = SnappyPrms.getDMLOperations();
      switch (DMLOp.getOperation(operation)) {
        case INSERT:
          Log.getLogWriter().info("Test will perform insert operation.");
          performInsert(conn, dConn);
          break;
        case UPDATE:
          Log.getLogWriter().info("Test will perform update operation.");
          performUpdate(conn,dConn);
          break;
        case DELETE:
          Log.getLogWriter().info("Test will perform delete operation.");
          performDelete(conn,dConn);
          break;
        default: Log.getLogWriter().info("Invalid operation. ");
      }
      closeConnection(conn);
      if(hasDerbyServer)
        closeDiscConnection(dConn,false);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing DML Ops. Exception is : " ,se);
    } catch (InterruptedException ie) {
      throw new TestException("Got exception while performing DML Ops. Exception is :", ie);
    }

  }

  public static synchronized void replayOpsInDerby(String opType, Connection dConn) throws
      SQLException {
    List<String> derbyOps;
    if (opType.equals("select")) {
      Log.getLogWriter().info("Pausing derby Op.");
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.PauseDerby);
    }
    if (SnappyBB.getBB().getSharedMap().containsKey("derbyOps")) {
      SnappyBB.getBB().getSharedLock().lock();
      derbyOps = (ArrayList<String>)SnappyBB.getBB().getSharedMap().get("derbyOps");
      SnappyBB.getBB().getSharedMap().remove("derbyOps");
      //SnappyBB.getBB().getSharedLock().unlock();
      if (derbyOps != null) {
        Log.getLogWriter().info("Performing Ops in derby from BB...");
        for (int i = 0; i < derbyOps.size(); i++) {
          //perform operation on derby
          String operation = derbyOps.get(i);
          Log.getLogWriter().info("Performing operation from BB in derby:" + operation);
          dConn.createStatement().execute(operation);
        }
        Log.getLogWriter().info("Performed " + derbyOps.size() + " Ops in derby from BB.");
      } else
        Log.getLogWriter().info("derbyOps is null");
    } else
      Log.getLogWriter().info("No Ops to perform in derby from BB");
  }

  public static synchronized void writeOpToBB(Connection dConn,String stmt) {
    List<String> derbyOps;
    Log.getLogWriter().info("Adding operation for derby in BB : " + stmt);
    //SnappyBB.getBB().getSharedLock().lock();
    if (SnappyBB.getBB().getSharedMap().containsKey("derbyOps")) {
      derbyOps = (ArrayList<String>)SnappyBB.getBB().getSharedMap().get("derbyOps");
      if(derbyOps==null)
        derbyOps = new ArrayList<String>();
    }
    else
      derbyOps = new ArrayList<String>();
    derbyOps.add(stmt);
    SnappyBB.getBB().getSharedMap().put("derbyOps", derbyOps);
    //SnappyBB.getBB().getSharedLock().unlock();
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
      String query = getSelectQueryForExecution();
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.BlockOps);
      ResultSet derbyRS = null;
      if (hasDerbyServer) {
        dConn = getDerbyConnection();
        replayOpsInDerby("select", dConn);
        //run select query in derby
        derbyRS = dConn.createStatement().executeQuery(query);
      } else
        Log.getLogWriter().info("derby server not started.");
      ResultSet snappyRS = conn.createStatement().executeQuery(query);
      SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.BlockOps);
      StructTypeImpl sti = ResultSetHelper.getStructType(derbyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, sti, true);
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, sti, true);
      compareResultSets(derbyList, snappyList);
      snappyRS.close();
      derbyRS.close();
      if (hasDerbyServer) {
        Log.getLogWriter().info("Releasing derby Ops.");
        SnappyBB.getBB().getSharedCounters().decrement(SnappyBB.PauseDerby);
        closeDiscConnection(dConn, false);
      }
      closeConnection(conn);

    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  public void  performInsert(Connection conn,Connection dConn) throws SQLException,
      InterruptedException{
    String insertStmt = getInsertStmt();
    TestHelper.waitForCounter(SnappyBB.getBB(),"SnappyBB.BlockOps", SnappyBB.BlockOps,
        0, true, -1, 1000);
    Log.getLogWriter().info("Inserting in snappy with statement : " + insertStmt);
    conn.createStatement().execute(insertStmt);
    Log.getLogWriter().info("Inserted row in snappy.");
    if(hasDerbyServer) {
      dConn = getDerbyConnection();
      if (SnappyBB.getBB().getSharedCounter("PauseDerby") == 0) {
        //no need to write op in BB, execute stmt in derby, but write previous ops first.
        replayOpsInDerby("dmlOp", dConn);
        Log.getLogWriter().info("Inserting in derby with statement :"+ insertStmt);
        dConn.createStatement().execute(insertStmt);
        Log.getLogWriter().info("Inserted row in derby.");
      } else {
        //need to write operation to BB
        writeOpToBB(dConn,insertStmt);
      }
    }
  }

  public void performUpdate(Connection conn,Connection dConn) throws SQLException{
    String updateStmt = getUpdateStmt();
    /*
    int numThreadsPerformingSelect = (int)SnappyBB.getBB().getSharedMap().get
        ("numThreadsPerformingSelect");
    TestHelper.waitForCounter(SnappyBB.getBB(),"SnappyBB.ReadyToBegin", SnappyBB.ReadyToBegin,
        numThreadsPerformingSelect,        true,        -1,        1000);
    */
    conn.createStatement().execute(updateStmt);
  }

  public void performDelete(Connection conn,Connection dConn) throws SQLException{
    String deleteStmt = getDeleteStmt();
    /*
    int numThreadsPerformingSelect = (int)SnappyBB.getBB().getSharedMap().get
        ("numThreadsPerformingSelect");
    TestHelper.waitForCounter(SnappyBB.getBB(),"SnappyBB.ReadyToBegin", SnappyBB.ReadyToBegin,
        numThreadsPerformingSelect,        true,        -1,        1000);
    */
    conn.createStatement().execute(deleteStmt);
  }

  public String getSelectQueryForExecution(){
    return "select * from app.shippers";
  }

  public String getInsertStmt(){
    Long insertCounter = SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.insertCounter);

    String insert = "insert into app.shippers values(" + insertCounter + ",'company" + insertCounter
        + "','" + insertCounter + insertCounter + "')";
    return insert;
  }

  public String getUpdateStmt(){
    return "update1";
  }

  public String getDeleteStmt(){
    return "delete1";
  }

  public void verifyResults(List<Struct> rs, List<Struct> rs1) {
    //verify results - check for duplicate rows for inserts
    ResultSetHelper.compareResultSets(rs, rs1);
    //checkForDuplicateRows();
    //checkforValidData();
  }

  public void checkForDuplicateRows(){

  }

  public void checkforValidData(){

  }

  /*
  Hydra task to perform bulk DMLOps which can be batch insert, batch update,bulk delete
  */
  public static void HydraTask_performBulkDMLOp() {
    testInstance.performBulkDMLOp();
  }

  public void performBulkDMLOp() {
    try {
      Connection conn = getLocatorConnection();
      String operation = SnappyPrms.getDMLOperations();
      switch (DMLOp.getOperation(operation)) {
        case INSERT: performBatchInsert(conn);
          break;
        case UPDATE: performBatchUpdate(conn);
          break;
        case DELETE: performBulkDelete(conn);
          break;
      }
      //perform bulk DML operation which can be insert, update, delete.
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing DML Ops...");
    }
  }

  public void performBatchInsert(Connection conn) throws SQLException{
    Statement bstmt = conn.createStatement();
    String insertStmt ;
    for(int i = 0; i< 10; i++) {
      insertStmt = getInsertStmt();
      bstmt.addBatch(insertStmt);
    }
    bstmt.executeBatch();
  }

  public void performBatchUpdate(Connection conn) throws SQLException{
    Statement bstmt = conn.createStatement();
    String updateStmt ;
    for(int i = 0; i< 10; i++) {
      updateStmt = getUpdateStmt();
      bstmt.addBatch(updateStmt);
    }
    bstmt.executeBatch();
  }

  public void performBulkDelete(Connection conn) throws SQLException{
    Statement bstmt = conn.createStatement();
    String deleteStmt ;
    for(int i = 0; i< 10; i++) {
      deleteStmt = getDeleteStmt();
      bstmt.addBatch(deleteStmt);
    }
    bstmt.executeBatch();
  }

  public static void compareResultSets(List<Struct> derbyResultSet,
      List<Struct> snappyResultSet) {
    compareResultSets(derbyResultSet, snappyResultSet, "derby", "snappy");
  }

  public static void compareResultSets(List<Struct> firstResultSet,
      List<Struct> secondResultSet, String first, String second) {
    Log.getLogWriter().info("size of resultSet from " + first + " is " + firstResultSet.size());
    Log.getLogWriter().info("Result from " + first + " is :" + listToString(firstResultSet));
    Log.getLogWriter().info("size of resultSet from " + second + " is " + secondResultSet.size());
    Log.getLogWriter().info("Result from " + second + " is :" + listToString(secondResultSet));

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
      for (int i = 0; i < derbyOps.size(); i++)
        Log.getLogWriter().info(derbyOps.get(i));
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
    closeDiscConnection(conn,false);
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

  public static synchronized void HydraTask_createDerbyTables(){
    testInstance.createDerbyTables();
  }

  protected void createDerbyTables() {
    if (!hasDerbyServer) {
      return;
    }
    Connection conn = getDerbyConnection();
    log().info("Creating tables in derby db.");
    createTables(conn);
    loadDerbyTables(conn);
    log().info("Done creating tables in derby db.");
    closeDiscConnection(conn,false);
  }

  /**
   * To create tables in derby
   */
  protected void createTables(Connection conn) {
    String driver;
    String url;
    try {
      driver = conn.getMetaData().getDriverName();
      url = conn.getMetaData().getURL();
      Log.getLogWriter().info("Driver name is " + driver + " url is " + url);
    } catch (SQLException se) {
      throw new TestException("Not able to get driver name" + TestHelper.getStackTrace(se));
    }
    //to get create table statements from config file
    String[] derbyTables = getCreateTablesStatements(true);
    String[] tableNames = getTableNames();

    try {
      Statement s = conn.createStatement();
      if (url.equals(DiscDBManager.getUrl()) || url.equals(ClientDiscDBManager.getUrl())) {
        for (int i = 0; i < derbyTables.length; i++) {
          Log.getLogWriter().info("about to create table : " + derbyTables[i]);
          s.execute(derbyTables[i]);
          Log.getLogWriter().info("Created table " + derbyTables[i]);
        }
      } else {
        throw new TestException("Got incorrect url or setting.");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create tables\n"
          + TestHelper.getStackTrace(se));
    }
    StringBuffer aStr = new StringBuffer("Created tables \n");
    if (url.equals(DiscDBManager.getUrl()) || url.equals(ClientDiscDBManager.getUrl())) {
      for (int i = 0; i < tableNames.length; i++) {
        aStr.append(tableNames[i] + "\n");
      }
    }
    Log.getLogWriter().info(aStr.toString());
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

  public static String[] getTableNames() {
    Long key = SQLPrms.dmlTables;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }

  public void loadDerbyTables(Connection conn){
    String[] tableNames = getTableNames();
    String[] csvFileNames = SnappyPrms.getCSVFileLocations();
    String dataLocation = SnappyPrms.getDataLocations();
    for (int i = 0; i < tableNames.length; i++) {
      String tableName = tableNames[i].toUpperCase();
      Log.getLogWriter().info("Loading data into "+ tableName);
      String[] table = tableName.split("\\.");
      //String csvFilePath = dataLocations[i];
      String csvFilePath = dataLocation + File.separator + csvFileNames[i];
      Log.getLogWriter().info("CSV location is : " + csvFilePath);
      try {
        PreparedStatement ps = conn.prepareStatement("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE(?,?,?,?,?,?,?)");
        ps.setString(1, table[0]);
        ps.setString(2, table[1]);
        ps.setString(3, csvFilePath);
        ps.setString(4, ",");
        ps.setString(5,null);
        ps.setString(6, null);
        ps.setInt(7, 0);
        ps.execute();
        Log.getLogWriter().info("Loaded data into "+ tableNames[i]);
      } catch (SQLException se) {
        throw new TestException("Exception while loading data to derby table.Exception is " + se
            .getSQLState() + " : " + se.getMessage());
      }
    }
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

    if (conn == null || (Boolean)resetDerbyConnection.get()) {
      Log.getLogWriter().info("derbyConnection is not set yet");
      try {
          conn = ClientDiscDBManager.getConnection();
      } catch(SQLException e){
        SQLHelper.printSQLException(e);
        throw new TestException("Not able to get Derby Connection:\n " + TestHelper.getStackTrace(e));
      }
      derbyConnection.set(conn);
      resetDerbyConnection.set(false);
    }

    return conn;
  }

  protected void closeDiscConnection(Connection conn, boolean end) {
    //close the connection at end of the test
    if (end) {
      try {
        conn.close();
        Log.getLogWriter().info("closing the connection");
      } catch (SQLException e) {
        SQLHelper.printSQLException(e);
        throw new TestException ("Not able to release the connection " + TestHelper.getStackTrace(e));
      }
    }
  }

}
