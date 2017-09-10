package io.snappydata.hydra.snapshotIsolation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import hydra.HostHelper;
import hydra.Log;
import hydra.PortHelper;
import io.snappydata.Server;
import io.snappydata.ServerManager;
import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

public class SnapshotIsolationWithTestHook extends SnapshotIsolationTest {

  final ScanTestHook testHook = new ScanTestHook();

  public static SnapshotIsolationWithTestHook testHookInstance;

  public static void HydraTask_initializeTestHook() {
    if (testHookInstance == null) {
      testHookInstance = new SnapshotIsolationWithTestHook();
      testHookInstance.initializeTestHook();
    }
  }

  public void initializeTestHook() {
    String addr = HostHelper.getHostAddress();
    int port = PortHelper.getRandomPort();
    String endpoint = addr + ":" + port;
    Log.getLogWriter().info("Generated peer server endpoint: " + endpoint);
    Properties serverProps = new Properties();
    String locatorsList = SnappyTest.getLocatorsList("locators");
    serverProps.setProperty("locators", locatorsList);
    serverProps.setProperty("mcast-port", "0");
    Server server = ServerManager.getServerInstance();
    try {
      server.start(serverProps);
    } catch (SQLException se) {
      throw new TestException("Error starting server.", se);
    }
    String url = "jdbc:snappydata:";
    String driver =  "io.snappydata.jdbc.EmbeddedDriver";
    loadDriver(driver);
    try {
      Properties props = new Properties();
      props.setProperty("mcast-port", "0");
      Connection conn = DriverManager.getConnection(url,props);
      Log.getLogWriter().info("Obtained connection");
      createTables(conn,false);
      saveTableMetaDataToBB(conn);
    } catch (SQLException se) {
      throw new TestException("Got Exception while getting connection.", se);
    }
  }

  /*
 Hydra task to perform DMLOps which can be insert, update, delete
 */
  public static void HydraTask_performDMLOp() {
    testHookInstance.performInsertUsingTestHook();
  }

  public void performInsertUsingTestHook() {
    try {
      String url = "jdbc:snappydata:";
      Connection conn = DriverManager.getConnection(url);
      waitForBarrier(2);
      GemFireCacheImpl.getInstance().waitOnScanTestHook();
      int numRowsInserted = (int)SnapshotIsolationBB.getBB().getSharedCounters().read
          (SnapshotIsolationBB.numRowsInserted);
      Log.getLogWriter().info("Number of rows before insert " + numRowsInserted);
      int insertCount = doInsert(conn);
      GemFireCacheImpl.getInstance().notifyRowScanTestHook();
      Misc.getGemFireCache().setRowScanTestHook(null);
      SnapshotIsolationBB.getBB().getSharedCounters().add(SnapshotIsolationBB.numRowsInserted,
          insertCount);
      Log.getLogWriter().info("Number of rows after insert " + (numRowsInserted + insertCount));
    } catch (SQLException se) {
      Log.getLogWriter().info(" Got Exception while inserting.");
      throw new TestException("Got exception while inserting", se);
    }
  }

  public int doInsert(Connection conn) throws SQLException {
    String[] dmlTable = SnapshotIsolationPrms.getDMLTables();
    int rand = new Random().nextInt(dmlTable.length);
    String tableName = dmlTable[rand];
    String row = getRowFromCSV(tableName, rand);
    if (testUniqueKeys)
      row = row + "," + getMyTid();
    Log.getLogWriter().info("Selected row is : " + row);
    PreparedStatement snappyPS = null;
    String insertStmt = SnapshotIsolationPrms.getInsertStmts()[rand];
    snappyPS = getPreparedStatement(conn, null, tableName, insertStmt, row);
    Log.getLogWriter().info("Inserting in snappy with statement : " + insertStmt + " with values("
        + row + ")");
    int rowCount = snappyPS.executeUpdate();
    Log.getLogWriter().info("Inserted " + rowCount + " row in snappy.");
    snappyPS.close();

    Log.getLogWriter().info("Done performing insert operation.");
    return rowCount;
  }

  public int doBatchInsert() {
    int batchSize = 100;
    try {
      Connection conn = getLocatorConnection();
      String[] dmlTable = SnapshotIsolationPrms.getDMLTables();
      int n = new Random().nextInt(dmlTable.length);
      String tableName = dmlTable[n];
      PreparedStatement snappyPS = null;
      String insertStmt = SnapshotIsolationPrms.getInsertStmts()[n];
      Log.getLogWriter().info("Performing batch insert on table " + tableName + " with batch size: " +
          batchSize);
      for (int i = 0; i < batchSize; i++) {
        String row = getRowFromCSV(tableName, n);
        if (testUniqueKeys)
          row = row + "," + getMyTid();
        snappyPS = getPreparedStatement(conn, snappyPS, tableName, insertStmt, row);
        snappyPS.addBatch();
      }
      Log.getLogWriter().info("Executing batch insert in snappy");
      int updateCnt[] =  snappyPS.executeBatch();
      Log.getLogWriter().info("Inserted " + updateCnt.length + " rows.");
    }catch(SQLException se){
      throw new TestException("Got exception while performing batch insert.",se);
    }
    return batchSize;
  }
  /*
   Hydra task to execute select queries
  */
  public static void HydraTask_executeQueries() {
    testHookInstance.executeQuery();
  }

  public void executeQuery() {

    try {
      String url = "jdbc:snappydata:";
      Connection conn = DriverManager.getConnection(url);
      Misc.getGemFireCache().setRowScanTestHook(testHook);
      waitForBarrier(2);
      Thread.sleep(500);
      int rowCnt = 0, numRowsInserted = 0;
      String query = SnapshotIsolationPrms.getSelectStmts();
      ResultSet snappyRS;
      Log.getLogWriter().info("Executing " + query + " on snappy.");
      try {
        numRowsInserted = (int)SnapshotIsolationBB.getBB().getSharedCounters().read
            (SnapshotIsolationBB.numRowsInserted);
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
      while(snappyRS.next())
        rowCnt += 1;
      snappyRS.close();
      if(rowCnt != numRowsInserted)
        throw new TestException("Select query has different row count. Number of rows from query " +
            "is " + rowCnt + " but expected number of rows are " + numRowsInserted);
      closeConnection(conn);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
    } catch(InterruptedException ie){
      throw new TestException("Got exception in sleep.", ie);
    }
  }

  class ScanTestHook implements GemFireCacheImpl.RowScanTestHook {
    Object lockForTest = new Object();
    Object operationLock = new Object();

    public void notifyOperationLock() {
      synchronized (operationLock) {
        operationLock.notify();
        Log.getLogWriter().info("Notification sent to select");
      }
    }

    public void notifyTestLock() {
      synchronized (lockForTest) {
        lockForTest.notify();
        Log.getLogWriter().info("Notification sent to insert");
      }
    }

    public void waitOnTestLock() {
      try {
        synchronized (lockForTest) {
          Log.getLogWriter().info("Insert statement is waiting");
          lockForTest.wait(20000);
        }
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }

    public void waitOnOperationLock() {
      try {
        synchronized (operationLock) {
          Log.getLogWriter().info("Select statement waiting");
          operationLock.wait(20000);
        }
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }
}
