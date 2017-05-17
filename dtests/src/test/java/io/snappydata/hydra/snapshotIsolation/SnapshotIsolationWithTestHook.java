package io.snappydata.hydra.snapshotIsolation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import hydra.Log;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class SnapshotIsolationWithTestHook extends SnapshotIsolationTest {

  public static SnapshotIsolationWithTestHook testHookInstance;

  public static void HydraTask_initializeTestHook() {
    if (testHookInstance == null) {
      testHookInstance = new SnapshotIsolationWithTestHook();
      testHookInstance.initializeTestHook();
    }
  }

  public void initializeTestHook()
  {
    ScanTestHook testHook = new ScanTestHook();
    GemFireCacheImpl.getInstance().setRowScanTestHook(testHook);
  }

  /*
 Hydra task to perform DMLOps which can be insert, update, delete
 */
  public static void HydraTask_performDMLOp() {
    testHookInstance.performInsertUsingTestHook();
  }

  public void performInsertUsingTestHook() {
    try {
      Connection conn = getLocatorConnection();
      GemFireCacheImpl.getInstance().waitOnScanTestHook();
      performInsert(conn);
      GemFireCacheImpl.getInstance().notifyRowScanTestHook();
    }catch(SQLException se){
      Log.getLogWriter().info(" Got Exception while inserting.");
      throw new TestException("Got exception while inserting", se);
    }
  }

  public void performInsert(Connection conn) throws SQLException {
    Connection dConn = null;
    String[] dmlTable = SnapshotIsolationPrms.getDMLTables();
    int rand = new Random().nextInt(dmlTable.length);
    String tableName = dmlTable[rand];
    String row = getRowFromCSV(tableName, rand);
    if (testUniqueKeys)
      row = row + "," + getMyTid();
    Log.getLogWriter().info("Selected row is : " + row);
    PreparedStatement snappyPS, derbyPS = null;
    String insertStmt = SnapshotIsolationPrms.getInsertStmts()[rand];
    snappyPS = getPreparedStatement(conn, null, tableName, insertStmt, row);

    Log.getLogWriter().info("Inserting in snappy with statement : " + insertStmt + " with values("
        + row + ")");
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Inserted " + rowCount + " row in snappy.");
    snappyPS.close();

    if (hasDerbyServer) {
      dConn = getDerbyConnection();
      derbyPS = getPreparedStatement(dConn, null, tableName, insertStmt, row);
      Log.getLogWriter().info("Inserting in derby with statement : " + insertStmt + " with " +
          "values(" + row + ")");
      rowCount = derbyPS.executeUpdate();
      Log.getLogWriter().info("Inserted " + rowCount + " row in derby.");
      derbyPS.close();
    }
    if (dConn != null)
      closeDiscConnection(dConn, true);
    Log.getLogWriter().info("Done performing insert operation.");
  }

  public void executeQuery() {
    try {
      Connection conn = getLocatorConnection();
      Connection dConn = null;
      String query = SnapshotIsolationPrms.getSelectStmts();
      ResultSet snappyRS;
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      try {
        snappyRS = conn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on snappy.");
      } catch (SQLException se) {
        if (se.getSQLState().equals("21000")) {
          //retry select query with routing
          Log.getLogWriter().info("Got exception while executing select query, retrying with " +
              "executionEngine as spark.");
          String query1 = query + " --GEMFIREXD-PROPERTIES executionEngine=Spark";
          snappyRS = conn.createStatement().executeQuery(query1);
          Log.getLogWriter().info("Executed query on snappy.");
        } else throw new SQLException(se);
      }
      ResultSet derbyRS = null;
      if (hasDerbyServer) {
        dConn = getDerbyConnection();
        //run select query in derby
        Log.getLogWriter().info("Executing " + query + " on derby.");
        derbyRS = dConn.createStatement().executeQuery(query);
        Log.getLogWriter().info("Executed query on derby.");
      }

      StructTypeImpl sti = ResultSetHelper.getStructType(derbyRS);
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, sti, true);
      StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRS);
      List<Struct> snappyList = ResultSetHelper.asList(snappyRS, snappySti, false);

      compareResultSets(derbyList, snappyList);
      snappyRS.close();
      derbyRS.close();
      if (dConn != null) {
        closeDiscConnection(dConn, true);
      }
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    }
  }

  class ScanTestHook implements GemFireCacheImpl.RowScanTestHook {
    Object lockForTest = new Object();
    Object operationLock = new Object();

    public void notifyOperationLock() {
      synchronized (operationLock) {
        operationLock.notify();
      }
    }

    public void notifyTestLock() {
      synchronized (lockForTest) {
        lockForTest.notify();
      }
    }

    public void waitOnTestLock() {
      try {
        synchronized (lockForTest) {
          lockForTest.wait();
        }
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }

    public void waitOnOperationLock() {
      try {
        synchronized (operationLock) {
          operationLock.wait();
        }
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }
}
