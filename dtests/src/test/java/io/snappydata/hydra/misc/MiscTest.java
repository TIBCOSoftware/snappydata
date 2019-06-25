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
package io.snappydata.hydra.misc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappySchemaPrms;
import util.TestException;

public class MiscTest extends SnappyTest {
  protected static MiscTest miscTestInst;

  public static void HydraTask_verify_snap2269_snap2762() {
    if (miscTestInst == null)
      miscTestInst = new MiscTest();
    miscTestInst.executeSqls();
  }

  public void executeSqls() {
    Connection conn;
    int[] colCnt;
    ResultSet rs = null;
    try {
      conn = getLocatorConnection();
    } catch (SQLException se) {
      throw new TestException("Got exception while obtaining conneciton..", se);
    }
    List<String> tableNames = Arrays.asList(SnappySchemaPrms.getTableNames());

    String[] selectStmts = SnappySchemaPrms.getSelectStmts();
    String[] ddlStmts = SnappySchemaPrms.getDDLStmts();
    String sql = "";

    createTables(conn);
    colCnt = new int[tableNames.size()];
    for (int i = 0; i < tableNames.size(); i++) {
      sql = "SELECT * FROM " + tableNames.get(i);
      try {
        rs = conn.createStatement().executeQuery(sql);
        colCnt[i] = rs.getMetaData().getColumnCount();
      } catch (SQLException se) {
        throw new TestException("Got exception while executing statement : " + sql, se);
      }
    }
    insertData(conn);
    String tabName = "";
    for (int j = 0; j < ddlStmts.length; j++) {
      sql = ddlStmts[j];
      try {
        conn.createStatement().execute(sql);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing ddl.. ", se);
      }
      if (sql.toLowerCase().startsWith("alter")) {
        tabName = sql.split(" ")[2];
        if (sql.toLowerCase().contains("add column"))
          colCnt[tableNames.indexOf(tabName)]++;
        if (sql.toLowerCase().contains("drop column"))
          colCnt[tableNames.indexOf(tabName)]--;
      }
      //select statement execution after alter stmt
      for (int i = 0; i < selectStmts.length; i++) {
        sql = selectStmts[i];
        try {
          rs = conn.createStatement().executeQuery(sql);
          int queryColCnt = 0;
          if (sql.toLowerCase().startsWith("select")) {
            queryColCnt = rs.getMetaData().getColumnCount();
            tabName = sql.split(" ")[3];//find table name from query ;
          } else { //describe
            while (rs.next())
              queryColCnt++;
            tabName = sql.split(" ")[1];//find table name from query ;
          }
          if (colCnt[tableNames.indexOf(tabName)] != queryColCnt)
            throw new TestException("Column count doesnot match after alter table.");
        } catch (SQLException se) {
          throw new TestException("Got exception while executing statement : " + sql, se);
        }
      }
    }
  }

  public static void HydraTask_verify_snap3007() {
    if (miscTestInst == null)
      miscTestInst = new MiscTest();
    miscTestInst.verify_3007();
  }

  public void verify_3007 () {
    try {
      Connection conn = getLocatorConnection();
      createTables(conn);
      insertData(conn);
      // check delete with createStatement.execute
      deleteWithoutPS(conn);
      verifyRowCount(conn,"createStatement.execute");
      //reinsert data
      insertData(conn);
      // check delete with preparedStatement.execute
      deleteWithPS(conn, false);
      verifyRowCount(conn, "preparedStatement.execute");
      //reinsert data
      insertData(conn);
      // check delete with preparedStatement.addBatch
      deleteWithPS(conn, true);
      verifyRowCount(conn, "preparedStatement.addBatch");
    } catch (SQLException se) {
      throw new TestException("Got SQLException ", se);
    }
  }

  public void createTables(Connection conn) {
    //create tables
    String sql = "";
    String[] createTables = SnappySchemaPrms.getCreateTablesStatements();
    for (int i = 0; i < createTables.length; i++) {
      sql = createTables[i];
      try {
        conn.createStatement().execute(sql);
      } catch (SQLException se) {
        throw new TestException("Got exception while creating tables", se);
      }
    }
  }

  public void insertData(Connection conn) {
    //insert records
    String sql = "";
    ArrayList<String> insertStmts = SnappySchemaPrms.getInsertStmts();
    for (int i = 0; i < insertStmts.size(); i++) {
      sql = insertStmts.get(i);
      try {
        conn.createStatement().execute(sql);
      } catch (SQLException se) {
        throw new TestException("Got exception while inserting data", se);
      }
    }
  }

  public void verifyRowCount(Connection conn, String afterMode) throws SQLException {
    Statement stmt = conn.createStatement();
    String sql = "SELECT COUNT(*) FROM ";
    String[] tabNames = SnappySchemaPrms.getTableNames();
    for (int i = 0; i < tabNames.length; i++) {
      sql = sql + tabNames[i];
      Log.getLogWriter().info("Executing select statement..." + sql);
      ResultSet rs = stmt.executeQuery(sql);
      rs.next();
      int count = rs.getInt(1);
      Log.getLogWriter().info("Number of rows in the table is : " + count);
      if(count>0) {
        throw new TestException("Delete statement failed to delete rows for " + tabNames[i] +
            " using " + afterMode);
      }
    }
    stmt.close();
  }

  public void deleteWithPS(Connection conn, boolean batchMode) throws SQLException {
    PreparedStatement ps = null;
    String sql = "DELETE FROM ";
    String[] tabNames = SnappySchemaPrms.getTableNames();
    for (int i = 0; i < tabNames.length; i++) {
      sql = sql + tabNames[i];
      Log.getLogWriter().info("Executing delete statement : " + sql);
      ps = conn.prepareStatement(sql);
      if(batchMode) {
        ps.addBatch();
        ps.executeBatch();
      } else ps.execute();
    }
    Log.getLogWriter().info("Executed delete.");
    ps.close();
  }

  public void deleteWithoutPS(Connection conn) throws SQLException {
    String sql = "DELETE FROM ";
    String[] tabNames = SnappySchemaPrms.getTableNames();
    for (int i = 0; i < tabNames.length; i++) {
      sql = sql + tabNames[i];
      Log.getLogWriter().info("Executing delete statement : " + sql);
      int numRows = conn.createStatement().executeUpdate(sql);
      Log.getLogWriter().info("Rows deleted : " + numRows );
    }
  }
}
