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

import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

public class MiscTest extends SnappyTest {
  protected static MiscTest miscTestInst;
  int colCnt = 6;


  //sql stmt for SNAP-2269 and SNAP-2762
  String[] sqlStmts = {
      "CREATE TABLE trade.portfolio (cid INT NOT NULL, sid INT NOT NULL, qty INT NOT NULL, " +
          "availQty INT NOT NULL, subTotal DECIMAL(30,20), tid INT, constraint portf_pk PRIMARY " +
          "KEY (cid, sid), constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0" +
          " AND availQty<=qty)) USING ROW OPTIONS(partition_by 'qty',PERSISTENT 'synchronous')",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(7,7,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(1,1,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(2,2,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(3,3,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(4,4,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(5,5,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(6,7,25,12,123.7,2)",
      "INSERT INTO trade.portfolio(cid,sid,qty,availqty,subtotal,tid) VALUES(6,6,25,12,123.7,2)",
      "SELECT * FROM trade.portfolio",
      "DESCRIBE trade.portfolio",
      "ALTER TABLE trade.portfolio ADD COLUMN name VARCHAR(10) NOT NULL default 'name'",
      "DESCRIBE trade.protfolio",
      "SELECT * FROM trade.portfolio",
      "ALTER TABLE trade.portfolio ADD COLUMN name1 VARCHAR(10)",
      "SELECT * FROM trade.portfolio",
      "DESCRIBE trade.portfolio",
      "ALTER TABLE trade.portfolio DROP COLUMN name1",
      "DESCRIBE trade.portfolio"
  };

  public static void HydraTask_verify_snap2269_snap2762() {
    if (miscTestInst == null)
      miscTestInst = new MiscTest();
    miscTestInst.executeSqls();
  }

  public void executeSqls() {
    Connection conn;
    ResultSet rs = null;
    try {
      conn = getLocatorConnection();
    } catch (SQLException se) {
      throw new TestException("Got exception while obtaining conneciton..", se);
    }

    for (int i = 0; i < sqlStmts.length; i++) {
      String sql = sqlStmts[i];
      if (sql.toLowerCase().startsWith("select") || sql.toLowerCase().startsWith("describe")) {
        try {
          rs = conn.createStatement().executeQuery(sql);
        } catch (SQLException se) {
          throw new TestException("Got exception while executing statement : " + sql, se);
        }
        if (sql.toLowerCase().startsWith("select")) {
          int queryColCnt = 0;
          try {
            queryColCnt = rs.getMetaData().getColumnCount();
          } catch (SQLException se) {
            throw new TestException("Got exception while executing statement : " + sql, se);
          }
          if (colCnt != queryColCnt)
            throw new TestException("Column count does not match after alter table.");
        } else {
          try {
            int queryColCnt = 0;
            while (rs.next()) {
              queryColCnt++;
            }
            if (colCnt != queryColCnt)
              throw new TestException("Column count doesnot match after alter table.");
          } catch (SQLException se) {
            throw new TestException("Got exception while executing statement : " + sql, se);
          }
        }
      } else if (sql.toLowerCase().startsWith("alter")) {
        if (sql.toLowerCase().contains("add column"))
          colCnt++;
        if (sql.toLowerCase().contains("drop column"))
          colCnt--;

      } else {
        try {
          conn.createStatement().execute(sql);
        } catch (SQLException se) {
          throw new TestException("Got exception while executing statement", se);
        }
      }
    }
  }

  public static void HydraTask_verify_snap3007() {
    if (miscTestInst == null)
      miscTestInst = new MiscTest();
    miscTestInst.verify_3007();
  }

  public void deleteWithoutPS(Connection conn) throws SQLException {
    System.out.println("Executing delete statement...");
    String sql = "DELETE FROM app.users";
    int numRows = conn.createStatement().executeUpdate(sql);
    System.out.println("Rows deleted : " + numRows + "\n");
  }

  public void reInsertData(Connection conn) throws SQLException{
    System.out.println("Executing insert statement...");
    String sql = "insert into users values(1,'abc',23),(2,'aaa',54),(3,'bbb',43),(4,'ccc',35)";
    conn.createStatement().execute(sql);
  }

  public int countRows(Connection conn) throws SQLException {
    Statement stmt = null;
    String sql = "SELECT count(*) FROM app.users";
    System.out.println("Executing select statement..." + sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    rs.next();
    int count = rs.getInt(1);
    stmt.close();
    return count;
  }

  public void deleteWithPS(Connection conn) throws SQLException {
    PreparedStatement ps = null;
    System.out.println("Executing delete statement...");
    String sql = "DELETE FROM app.users";
    ps = conn.prepareStatement(sql);
    ps.execute();
    System.out.println("Executed delete statement...");
    ps.close();
  }

  public void deleteWithBatch(Connection conn) throws SQLException {
    PreparedStatement ps = null;
    System.out.println("Executing delete statement...");
    String sql = "DELETE FROM app.users";
    ps = conn.prepareStatement(sql);
    ps.addBatch();
    ps.execute();
    System.out.println("Executed delete statement...");
    ps.close();
  }

  public void verify_3007 ()
  {
    try{
      Connection conn = getLocatorConnection();
      deleteWithoutPS(conn);
      int cnt = countRows(conn);
      if(cnt > 0 )
        throw new TestException("Unable to delete data from the table using delete stmt");
      reInsertData(conn);
      deleteWithPS(conn);
      cnt = countRows(conn);
      if(cnt > 0 )
        throw new TestException("Unable to delete data from the table using delete with prepared stmt");
      reInsertData(conn);
      deleteWithBatch(conn);
      cnt = countRows(conn);
      if(cnt > 0 )
        throw new TestException("Unable to delete data from the table using delete with addBatch");
    } catch (SQLException se) {
      throw new TestException("Got SQLException ", se);
    }
  }

}