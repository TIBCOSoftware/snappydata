
package io.snappydata.hydra.misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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

      if (sql.startsWith("select") || sql.startsWith("describe")) {
        try {
          rs = conn.createStatement().executeQuery(sql);
        } catch (SQLException se) {
          throw new TestException("Got exception while executing statement : " + sql, se);
        }
        if (sql.startsWith("select")) {
          int queryColCnt = 0;
          try {
            queryColCnt = rs.getMetaData().getColumnCount();
          } catch (SQLException se) {
            throw new TestException("Got exception while executing statement : " + sql, se);
          }
          if (colCnt != queryColCnt)
            throw new TestException("Column count doent match after alter table.");
        } else {
          try {
            int queryColCnt = 0;
            while (rs.next()) {
              queryColCnt++;
            }
            if (colCnt != queryColCnt)
              throw new TestException("Column count doent match after alter table.");
          } catch (SQLException se) {
            throw new TestException("Got exception while executing statement : " + sql, se);
          }
        }

      } else if (sql.startsWith("alter")) {
        if (sql.contains("add column"))
          colCnt++;
        if (sql.contains("drop column"))
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

}