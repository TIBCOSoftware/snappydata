/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import hydra.Log;
import hydra.Prms;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.snapshotIsolation.SnapshotIsolationDMLOpsBB;
import org.apache.commons.lang.ArrayUtils;
import sql.SQLHelper;
import sql.sqlutil.GFXDStructImpl;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class SnappyDMLOpsUtil extends SnappyTest {

  public static boolean hasDerbyServer = false;
  public static boolean testUniqueKeys = false;
  public static boolean isHATest = false;
  protected static hydra.blackboard.SharedLock dmlLock;

  protected static SnappyDMLOpsUtil testInstance;
  public static DerbyTestUtils derbyTestUtils;

  public static void HydraTask_initialize() {
    if (testInstance == null)
      testInstance = new SnappyDMLOpsUtil();
    hasDerbyServer = TestConfig.tab().booleanAt(Prms.manageDerbyServer, false);
    testUniqueKeys = TestConfig.tab().booleanAt(SnappySchemaPrms.testUniqueKeys, true);
    isHATest = TestConfig.tab().booleanAt(SnappySchemaPrms.isHATest,false);
    int dmlTableLength = SnappySchemaPrms.getDMLTables().length;
    ArrayList<Integer> insertCounters = new ArrayList<>();
    for (int i = 0; i < dmlTableLength; i++) {
      insertCounters.add(1);
    }
    if (!SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("insertCounters"))
      SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("insertCounters", insertCounters);
    derbyTestUtils = new DerbyTestUtils();
  }

  //ENUM for DML Ops
  public enum DMLOp {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

    String opType;

    DMLOp(String opType) {
      this.opType = opType;
    }

    public String getOpType() {
      return opType;
    }

    public static SnappyDMLOpsUtil.DMLOp getOperation(String dmlOp) {
      if (dmlOp.equals(INSERT.getOpType())) {
        return INSERT;
      } else if (dmlOp.equals(UPDATE.getOpType())) {
        return UPDATE;
      } else if (dmlOp.equals(DELETE.getOpType())) {
        return DELETE;
      } else return null;
    }
  }

  //ENUM for Connection Type
  public enum ConnType {
    JDBC("jdbc"),
    SNAPPY("snappy"),
    SMARTCONNECTOR("smartconnector");

    String connType;

    ConnType(String connType) {
      this.connType = connType;
    }

    public String getConnType() {
      return connType;
    }

    public static SnappyDMLOpsUtil.ConnType getOperation(String conn) {
      if (conn.equals(JDBC.getConnType())) {
        return JDBC;
      } else if (conn.equals(SNAPPY.getConnType())) {
        return SNAPPY;
      } else if (conn.equals(SMARTCONNECTOR.getConnType())) {
        return SMARTCONNECTOR;
      } else return null;
    }
  }

  public static void HydraTask_changeMetaDataDirForSpark(){
    String sparkDir = hd.getGemFireHome() + ".." + sep + ".." + sep + ".." + sep + "spark" + sep;
    String filePath = sparkDir + "launcher" + sep + "build-artifacts" + sep + "scala-2.11" +
        sep + "resources" + sep + "test" + sep + "spark-defaults.conf";
    try {
      File file = new File(filePath);
      FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write("derby.system.home=test_db");
      bw.newLine();
      bw.close();
    }
    catch(IOException ie){
      throw new TestException("Error while writing to spark file.");
    }
  }

  public static void HydraTask_initializeDMLThreads() {
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
    testInstance.saveTableMetaDataToBB();
  }

  public void saveTableMetaDataToBB() {
    try {
      Connection conn = getLocatorConnection();
      String[] tableNames = SnappySchemaPrms.getTableNames();
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
      throw new TestException("Got exception while saving table metadata to BB. Exception is : ",
          se);
    } catch (ClassNotFoundException ce) {
      throw new TestException("Got exception while saving table metadata to BB.. Exception is : ", ce);
    }
  }

  public static void HydraTask_createSnappySchemas() {
    testInstance.createSnappySchemas();
  }

  protected void createSnappySchemas() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("creating schemas in snappy.");
      createSchemas(conn, false);
      Log.getLogWriter().info("done creating schemas in snappy.");
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void createSchemas(Connection conn, boolean isDerby) {
    String[] schemas = SnappySchemaPrms.getSchemas();
    StringBuffer aStr = new StringBuffer("Created schemas \n");
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < schemas.length; i++) {
        s.execute(schemas[i]);
        aStr.append(schemas[i] + "\n");
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
    Log.getLogWriter().info(aStr.toString());
  }

  public static synchronized void HydraTask_createSnappyTables() {
    testInstance.createSnappyTables();
  }

  protected void createSnappyTables() {
    try {
      Connection conn = getLocatorConnection();
      Log.getLogWriter().info("dropping tables in snappy.");
      dropTables(conn); //drop table before creating it
      Log.getLogWriter().info("done dropping tables in snappy");
      Log.getLogWriter().info("creating tables in snappy.");
      createTables(conn, false);
      Log.getLogWriter().info("done creating tables in snappy.");
      //loadTables(conn);
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  protected void createTables(Connection conn, boolean isDerby) {
    //to get create table statements from config file
    String[] createTablesDDL = SnappySchemaPrms.getCreateTablesStatements();
    String[] ddlExtn = SnappySchemaPrms.getSnappyDDLExtn();
    StringBuffer aStr = new StringBuffer("Created tables \n");
    try {
      Statement s = conn.createStatement();
      String createDDL;
      for (int i = 0; i < createTablesDDL.length; i++) {
        if (isDerby)
          createDDL = createTablesDDL[i];
        else
          createDDL = createTablesDDL[i] + ddlExtn[i];
        Log.getLogWriter().info("About to create table : " + createDDL);
        s.execute(createDDL);
        Log.getLogWriter().info("Table created.");
        aStr.append(createDDL + "\n");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create tables\n"
          + TestHelper.getStackTrace(se));
    }
    Log.getLogWriter().info(aStr.toString());
  }

  public static synchronized void HydraTask_populateTables() {
    testInstance.populateTables();
  }

  protected void populateTables() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      if (hasDerbyServer)
        dConn = derbyTestUtils.getDerbyConnection();
      String[] tableNames = SnappySchemaPrms.getTableNames();
      String[] csvFileNames = SnappySchemaPrms.getCSVFileNames();
      String dataLocation = SnappySchemaPrms.getDataLocations();
      for (int i = 0; i < tableNames.length; i++) {
        String tableName = tableNames[i].toUpperCase();
        Log.getLogWriter().info("Loading data into " + tableName);
        String csvFilePath = dataLocation + File.separator + csvFileNames[i];
        Log.getLogWriter().info("CSV location is : " + csvFilePath);
        FileInputStream fs = new FileInputStream(csvFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs));
        String insertStmt = "insert into " + tableName + " values (";
        String row = null;

        ArrayList<Integer> dmlthreads = null;
        int tid;
        if (SnapshotIsolationDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
          dmlthreads = (ArrayList<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
        while ((row = br.readLine()) != null) {
          if (dmlthreads == null)
            tid = getMyTid();
          else
            tid = dmlthreads.get(new Random().nextInt(dmlthreads.size()));
          String rowStmt = insertStmt + row + "," + tid + ")";
          //Log.getLogWriter().info("Row is : " +  rowStmt);
          conn.createStatement().execute(rowStmt);
          dConn.createStatement().execute(rowStmt);
        }
        Log.getLogWriter().info("Done loading data into table " + tableName);
      }
      conn.close();
      derbyTestUtils.closeDiscConnection(dConn, true);
    } catch (IOException ie) {
      throw new TestException("Got exception while populating table.", ie);
    } catch (SQLException se) {
      throw new TestException("Got exception while populating table.", se);
    }
  }

  protected void dropTables(Connection conn) {
    String sql = null;
    String[] tables = SnappySchemaPrms.getTableNames();
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

  /*
   Hydra task to perform DMLOps which can be insert, update, delete
   */
  public static void HydraTask_performDMLOp() {
    testInstance.performDMLOp(ConnType.JDBC);
  }

  public void performDMLOp(ConnType connType) {
    //perform DML operation which can be insert, update, delete.
    String operation = SnappySchemaPrms.getDMLOperations();
    switch (SnappyDMLOpsUtil.DMLOp.getOperation(operation)) {
      case INSERT:
        Log.getLogWriter().info("Performing insert operation.");
        performInsert();
        break;
      case UPDATE:
        Log.getLogWriter().info("Performing update operation.");
        if (connType.equals(ConnType.JDBC))
          performUpdate();
        else
          performUpdateInSnappy(connType);
        break;
      case DELETE:
        Log.getLogWriter().info("Performing delete operation.");
        if (connType.equals(ConnType.JDBC))
          performDelete();
        else
          performDeleteInSnappy(connType);
        break;
      default:
        Log.getLogWriter().info("Invalid operation. ");
        throw new TestException("Invalid operation type.");
    }
  }

  public void performInsert() {
    try {
      Connection conn;
      if(SnappyPrms.setTx()) {
        conn = getLocatorConnection(false);
      } else {
        conn = getLocatorConnection();
      }
      Connection dConn = null;
      String[] dmlTable = SnappySchemaPrms.getDMLTables();
      int rand = new Random().nextInt(dmlTable.length);
      String tableName = dmlTable[rand];
      String row = getRowFromCSV(tableName, rand);
      if (testUniqueKeys)
        row = row + "," + getMyTid();

      //Log.getLogWriter().info("Selected row is : " + row);
      PreparedStatement snappyPS, derbyPS = null;
      String insertStmt = SnappySchemaPrms.getInsertStmts()[rand];
      snappyPS = getPreparedStatement(conn, null, tableName, insertStmt, row);
      Log.getLogWriter().info("Inserting in snappy : " + insertStmt + " with " +
          "values(" + row + ")");
      int rowCount = snappyPS.executeUpdate();
      Log.getLogWriter().info("Inserted " + rowCount + " row in snappy.");
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        derbyPS = getPreparedStatement(dConn, null, tableName, insertStmt, row);
        Log.getLogWriter().info("Inserting in derby : " + insertStmt + " with " +
            "values(" + row + ")");
        int derbyRowCount = derbyPS.executeUpdate();
        Log.getLogWriter().info("Inserted " + derbyRowCount + " row in derby.");
        if (rowCount != derbyRowCount)
          Log.getLogWriter().info("Insert statement failed to insert same rows in derby and " +
              "snappy. Derby inserted " + derbyRowCount + " and snappy inserted " + rowCount + ".");
        derbyTestUtils.closeDiscConnection(dConn, true);
      }
      closeConnection(conn);

    } catch (SQLException se) {
      throw new TestException("Got exception while performing insert operation.", se);
    }
  }

  public void performUpdate() {
    try {
      Connection conn;
      if(SnappyPrms.setTx()) {
        conn = getLocatorConnection(false);
      } else {
        conn = getLocatorConnection();
      }
      Connection dConn = null; //get the derby connection here
      String updateStmt[] = SnappySchemaPrms.getUpdateStmts();
      int numRows = 0;
      int rand = new Random().nextInt(updateStmt.length);
      String stmt = updateStmt[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid",""+ tid);
      if (testUniqueKeys) {
        if (stmt.toUpperCase().contains("WHERE"))
          stmt = stmt + " AND tid=" + tid;
        else stmt = stmt + " WHERE tid=" + tid;
      }
      Log.getLogWriter().info("Executing " + stmt + " on snappy.");
      numRows = conn.createStatement().executeUpdate(stmt);
      Log.getLogWriter().info("Updated " + numRows + " rows in snappy.");
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Updated " + derbyRows + " rows in derby.");
        if (numRows != derbyRows) {
          String errMsg = "Update statement failed to update same rows in derby and " +
              "snappy. Derby updated " + derbyRows + " and snappy updated " + numRows + ".";
          Log.getLogWriter().info(errMsg);
          throw new TestException(errMsg);
        }
        derbyTestUtils.closeDiscConnection(dConn, true);
        String tableName = SnappySchemaPrms.getUpdateTables()[rand];
        String message = verifyResultsForTable(tableName,true);
        if(message.length()!=0){
          throw new TestException("Validation failed after update table.");
        }
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing update operation.", se);
    }
  }

  public void performDelete() {
    try {
      Connection conn;
      if(SnappyPrms.setTx()) {
        conn = getLocatorConnection(false);
      } else {
        conn = getLocatorConnection();
      }
      Connection dConn = null; //get the derby connection here
      String deleteStmt[] = SnappySchemaPrms.getDeleteStmts();
      int numRows = 0;
      int rand = new Random().nextInt(deleteStmt.length);
      String stmt = deleteStmt[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid",""+ tid);
      if (testUniqueKeys) {
        if (stmt.toUpperCase().contains("WHERE"))
          stmt = stmt + " AND tid=" + tid;
        else stmt = stmt + " WHERE tid=" + tid;
      }
      Log.getLogWriter().info("Executing " + stmt + " on snappy.");
      numRows = conn.createStatement().executeUpdate(stmt);
      Log.getLogWriter().info("Deleted " + numRows + " rows in snappy.");
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Deleted " + derbyRows + " rows in derby.");
        if (numRows != derbyRows) {
          String errMsg = "Delete statement failed to delete same rows in derby and " +
              "snappy. Derby deleted " + derbyRows + " and snappy deleted " + numRows + ".";
          Log.getLogWriter().info(errMsg);
          throw new TestException(errMsg);
        }
        derbyTestUtils.closeDiscConnection(dConn, true);
        String tableName = SnappySchemaPrms.getDeleteTables()[rand];
        String message = verifyResultsForTable(tableName,true);
        if(message.length()!=0){
          throw new TestException("Validation failed after executing delete on table.");
        }
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while performing delete operation.", se);
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
      String selectStmt[] = SnappySchemaPrms.getSelectStmts();
      ResultSet snappyRS, derbyRS = null;
      int rand = new Random().nextInt(selectStmt.length);
      String query = selectStmt[rand];
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      try {
        snappyRS = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
      } catch (SQLException se) {
        if (se.getSQLState().equals("21000") || se.getSQLState().equals("0A000")) {
          //retry select query with routing
          Log.getLogWriter().info("Got exception while executing select query, retrying with " +
              "executionEngine as spark.");
          String query1 = query + " --GEMFIREXD-PROPERTIES executionEngine=Spark";
          snappyRS = conn.createStatement().executeQuery(query1);
          Log.getLogWriter().info("Executed query on snappy.");
        } else throw new SQLException(se);
      }
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);

      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + query + " on derby.");
        derbyRS = dConn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
        StructTypeImpl derbySti = ResultSetHelper.getStructType(derbyRS);
        List<Struct> derbyList = ResultSetHelper.asList(derbyRS, derbySti, true);
        compareResultSets(derbyList, snappyList);
        derbyRS.close();
        derbyTestUtils.closeDiscConnection(dConn, true);
      } else {
        int numRows = 0;
        while (snappyRS.next()) numRows++;
        Log.getLogWriter().info("Snappy returned " + numRows + " rows.");
      }
      snappyRS.close();
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
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
      throw new TestException(aStr.toString());
    }
    if (firstResultSet.size() == secondResultSet.size()) {
      Log.getLogWriter().info("verified that results are correct");
    } else if (firstResultSet.size() < secondResultSet.size()) {
      throw new TestException("There are more data in " + second + " ResultSet");
    } else {
      throw new TestException("There are fewer data in " + second + " ResultSet");
    }
  }

  public static String listToString(List<Struct> aList) {
    if (aList == null) {
      throw new TestException("test issue, need to check in the test and not pass in null list here");
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
  Verify results at the end of the test
  */
  public static void HydraTask_verifyResults() {
    testInstance.verifyResults();
  }

  public void verifyResults() {
    String[] tables = SnappySchemaPrms.getTableNames();
    StringBuffer mismatchString = new StringBuffer();
      for (String table : tables) {
        mismatchString.append(verifyResultsForTable(table,false));
      }
      if (mismatchString.length() > 0)
      throw new TestException(mismatchString.toString());
  }

  public String verifyResultsForTable(String table, boolean useTid) {
    StringBuffer mismatchString = new StringBuffer();
    Connection conn,dConn;
    try {
      conn = getLocatorConnection();
      dConn = derbyTestUtils.getDerbyConnection();
      String selectStmt = "select * from " + table;
      if(useTid)
        selectStmt = selectStmt + " where tid = " + getMyTid();
      Log.getLogWriter().info("Verifying results for " + table + " using " + selectStmt);
      ResultSet snappyRS = conn.createStatement().executeQuery(selectStmt);
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);
      ResultSet derbyRS = dConn.createStatement().executeQuery(selectStmt);
      StructTypeImpl derbySti = ResultSetHelper.getStructType(derbyRS);
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, derbySti, true);
      try {
        compareResultSets(derbyList, snappyList);
      } catch (TestException te) {
        mismatchString = mismatchString.append("Result mismatch in " + table + " :\n");
        mismatchString = mismatchString.append(te.getMessage()).append("\n");
      }
    } catch (SQLException se) {
      throw new TestException("Got Exception while verifying the table data.", se);
    }
    closeConnection(conn);
    derbyTestUtils.closeDiscConnection(dConn, true);
    return mismatchString.toString();
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

  public String getRowFromCSV(String tableName, int randTable) {
    String row = null;
    int insertCounter;
    String csvFilePath = SnappySchemaPrms.getCsvLocationforLargeData();
    String csvFileName = SnappySchemaPrms.getInsertCsvFileNames()[randTable];
    getDmlLock();
    List<Integer> counters = (List<Integer>)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
        ("insertCounters");
    insertCounter = counters.get(randTable);
    counters.set(randTable, insertCounter + 1);
    SnapshotIsolationDMLOpsBB.getBB().getSharedMap().put("insertCounters", counters);
    releaseDmlLock();
    //Log.getLogWriter().info("insert Counter is :" + insertCounter + " for csv " + csvFilePath +
    //    File.separator + csvFileName);
    try (Stream<String> lines = Files.lines(Paths.get(csvFilePath + File.separator + csvFileName))) {
      row = lines.skip(insertCounter).findFirst().get();
    } catch (IOException io) {
      throw new TestException("File not found at specified location " +
          (csvFilePath + File.separator + csvFileName));
    }
    return row;
  }

  protected void getDmlLock() {
    if (dmlLock == null)
      dmlLock = SnapshotIsolationDMLOpsBB.getBB().getSharedLock();
    dmlLock.lock();
  }

  protected void releaseDmlLock() {
    dmlLock.unlock();
  }

  public String getStmt(String stmt, String row, String tableName) {
    String[] columnValues = row.split(",");
    String replaceString = stmt;
    StructTypeImpl sType = (StructTypeImpl)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName);
    ObjectType[] oTypes = sType.getFieldTypes();
    for (int i = 0; i < oTypes.length; i++) {
      String clazz = oTypes[i].getSimpleClassName();
      String columnValue = columnValues[i];
      switch (clazz) {
        case "String":
          replaceString = setString(replaceString, columnValue);
          break;
        case "Timestamp":
          Timestamp ts = Timestamp.valueOf(columnValue);
          replaceString = setString(replaceString, ts.toString());
          break;
        case "Integer":
        case "Double":
          replaceString = setInt(replaceString, columnValue);
          break;

      }
    }
    Log.getLogWriter().info("Insert statement is :" + replaceString);
    return replaceString;
  }

  public String setString(String stmt, String value){
    stmt = stmt.replaceFirst("\\?", "'" + value +"'");
    return stmt;
  }

  public String setInt(String stmt, String value){
    stmt = stmt.replaceFirst("\\?",value);
    return stmt;
  }

  public static void HydraTask_performDMLOpsInJob(){
    testInstance.performDMLOp(ConnType.SNAPPY);
  }

  public static void HydraTask_performDMLOpsInApp(){
    testInstance.performDMLOp(ConnType.SMARTCONNECTOR);
  }

  public void performUpdateInSnappy(ConnType connType) {
    try {
      Connection dConn = null; //get the derby connection here
      String updateStmt[] = SnappySchemaPrms.getUpdateStmts();
      int rand = new Random().nextInt(updateStmt.length);
      String stmt = updateStmt[rand];
      String tableName = SnappySchemaPrms.getUpdateTables()[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid", ""+tid);
      if (testUniqueKeys) {
        if (stmt.toUpperCase().contains("WHERE"))
          stmt = stmt + " AND tid=" + tid;
        else stmt = stmt + " WHERE tid=" + tid;
      }

      if(connType.equals(ConnType.SNAPPY)) {
        dynamicAppProps.put(tid,"stmt=\\\"" + stmt + "\\\",tableName=" + tableName + ",tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      }
      else{ // thin client smart connector mode
        dynamicAppProps.put(tid,"\"" + stmt + "\""  + " " + tid);
        String logFile = "snappyAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Updated " + derbyRows + " rows in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String message = verifyResultsForTable(tableName, true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after update table.");
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while performing update operation.", se);
    }
  }

  public void performDeleteInSnappy(ConnType connType) {
    try {
      Connection dConn = null; //get the derby connection here
      String deleteStmt[] = SnappySchemaPrms.getDeleteStmts();
      int numRows = 0;
      int rand = new Random().nextInt(deleteStmt.length);
      String stmt = deleteStmt[rand];
      String tableName = SnappySchemaPrms.getDeleteTables()[rand];
      int tid = getMyTid();
      if (stmt.contains("$tid"))
        stmt = stmt.replace("$tid","" + tid);
      if (testUniqueKeys) {
        if (stmt.toUpperCase().contains("WHERE"))
          stmt = stmt + " AND tid=" + tid;
        else stmt = stmt + " WHERE tid=" + tid;
      }

      if (connType.equals(ConnType.SNAPPY)) {
        dynamicAppProps.put(tid,"stmt=\\\"" + stmt + "\\\",tableName=" + tableName + ",tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        dynamicAppProps.put(tid, "\"" + stmt + "\"" + " " + tid);
        String logFile = "snappyAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Executing " + stmt + " on derby.");
        int derbyRows = dConn.createStatement().executeUpdate(stmt);
        Log.getLogWriter().info("Deleted " + derbyRows + " rows in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String message = verifyResultsForTable(tableName,true);
        if(message.length()!=0){
          throw new TestException("Validation failed after executing delete on table.");
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while performing delete operation.", se);
    }
  }

  public void performInsertInSnappy(ConnType connType) {
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
        dynamicAppProps.put(tid,"stmt=\\\"" + insertStmt + "\\\",tableName=" + tableName + "," +
            "tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
            jarPath, SnappyPrms.getUserAppName());
      } else { // thin client smart connector mode
        dynamicAppProps.put(tid,"\"" + insertStmt + "\""  + " " + tid);
        String logFile = "snappyAppResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        executeSparkJob(SnappyPrms.getSparkJobClassNames(), logFile);
      }
      if (hasDerbyServer) {
        dConn = derbyTestUtils.getDerbyConnection();
        Log.getLogWriter().info("Inserting in derby : " + insertStmt );
        int derbyRowCount = dConn.createStatement().executeUpdate(insertStmt);
        Log.getLogWriter().info("Inserted " + derbyRowCount + " row in derby.");
        derbyTestUtils.closeDiscConnection(dConn, true);
        String message = verifyResultsForTable(tableName, true);
        if (message.length() != 0) {
          throw new TestException("Validation failed after insert in table " + tableName +".");
        }
      }
    } catch (SQLException se) {
      throw new TestException("Got exception while performing insert operation.", se);
    }
  }
  public String buildUpdateStmt(String tableName){
    String updateStmt = "update $tableName set $updateList where $whereClause";
    //String[] tables = SnappySchemaPrms.getDMLTables();
    //String tableName = tables[new Random().nextInt(tables.length)];
    updateStmt = updateStmt.replace("$tableName", tableName);
    String whereClause = "";
    int tid = getMyTid();
    StructTypeImpl sType = (StructTypeImpl)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName);
    String[] columnNames = sType.getFieldNames();
    ObjectType[] oTypes = sType.getFieldTypes();
    String updateList = "";
    int numColumnsToUpdate = new Random().nextInt(2);
    for(int i=0;i<numColumnsToUpdate;i++){
      if(updateList.length()!=0)
        updateList.concat(" , ");
      int randomInt = new Random().nextInt(columnNames.length);
      String updateColumn = columnNames[randomInt];
      ArrayUtils.remove(columnNames,randomInt);
      updateColumn.concat("=");

      updateList.concat("");
    }
    updateStmt = updateStmt.replace("$updateList", updateList);
    whereClause = "";
    if (whereClause.length() != 0)
      whereClause.concat(" AND ");
    whereClause.concat(" tid = " + tid);
    updateStmt = updateStmt.replace("$whereClause", whereClause);
    return updateStmt;
  }

  public String buildDeleteStmt(String tableName){
    String deleteStmt = "delete from $tableName where $whereClause";
    //String[] tables = SnappySchemaPrms.getDMLTables();
    //String tableName = tables[new Random().nextInt(tables.length)];
    deleteStmt = deleteStmt.replace("$tableName", tableName);
    String whereClause = "";
    int tid = getMyTid();
    StructTypeImpl sType = (StructTypeImpl)SnapshotIsolationDMLOpsBB.getBB().getSharedMap().get
        ("tableMetaData_" + tableName);
    String[] columnNames = sType.getFieldNames();
    ObjectType[] oTypes = sType.getFieldTypes();

    whereClause = "";
    if (whereClause.length() != 0)
      whereClause.concat(" AND ");
    whereClause.concat(" tid = " + tid);
    deleteStmt = deleteStmt.replace("$whereClause", whereClause);
    return deleteStmt;
  }

  //ENUM for where clause operator
  public enum WhereClauseOperator {
    LESSTHAN("<"),
    GREATERTHAN(">"),
    EQUALTO("=");

    String opType;

    WhereClauseOperator(String opType) {
      this.opType = opType;
    }

    public String getOpType() {
      return opType;
    }

    public static SnappyDMLOpsUtil.WhereClauseOperator getOperation(String op) {
      if (op.equals(LESSTHAN.getOpType())) {
        return LESSTHAN;
      } else if (op.equals(GREATERTHAN.getOpType())) {
        return GREATERTHAN;
      } else if (op.equals(EQUALTO.getOpType())) {
        return EQUALTO;
      } else return null;
    }
  }

  //ENUM for Connection Type
  public enum ExprOperator {
    ADDITION("+"),
    SUBTRACTION("-"),
    MULTIPLICATION("*");

    String opType;

    ExprOperator(String opType) {
      this.opType = opType;
    }

    public String getOpType() {
      return opType;
    }

    public static SnappyDMLOpsUtil.ExprOperator getOperation(String conn) {
      if (conn.equals(ADDITION.getOpType())) {
        return ADDITION;
      } else if (conn.equals(SUBTRACTION.getOpType())) {
        return SUBTRACTION;
      } else if (conn.equals(MULTIPLICATION.getOpType())) {
        return MULTIPLICATION;
      } else return null;
    }
  }
}
