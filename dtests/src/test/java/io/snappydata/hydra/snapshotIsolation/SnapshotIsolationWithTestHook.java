package io.snappydata.hydra.snapshotIsolation;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import com.pivotal.gemfirexd.internal.engine.Misc;
import hydra.Log;
import hydra.blackboard.AnyCyclicBarrier;
import io.snappydata.Server;
import io.snappydata.ServiceManager;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyTest;
import util.TestException;

public class SnapshotIsolationWithTestHook extends SnapshotIsolationTest {

  final ScanTestHook testHook = new ScanTestHook();

  public static SnapshotIsolationWithTestHook testHookInstance;

  public static void HydraTask_startSnappyServerAPI() {
    if (testHookInstance == null) {
      testHookInstance = new SnapshotIsolationWithTestHook();
      testHookInstance.startSnappyServerAPI();
    }
  }

  public void startSnappyServerAPI() {
    String path = "";
    try{
      path = new File(".").getCanonicalPath() + File.separator + "server" + getMyTid();
    } catch(IOException ie){
      throw new TestException("Got exception while accessing current directory", ie);
    }
    Properties serverProps = new Properties();
    String locatorsList = SnappyTest.getLocatorsList("locators");
    serverProps.setProperty("locators", locatorsList);
    //serverProps.setProperty("mcast-port", "0");
    serverProps.setProperty("log-file", path + File.separator + "snappyStore.log");
    serverProps.setProperty("log-level", "fine");
    serverProps.setProperty("spark.executor.cores","8");
    serverProps.setProperty("spark.memory.manager",
        "org.apache.spark.memory.SnappyUnifiedMemoryManager");
    serverProps.setProperty("sys-disk-dir",path);
    startSnappyServer(serverProps);

  }


  public static void HydraTask_createTableAndInitialize() {
    int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.snappyClusterStarted);
    if (num == 1) {
/*      String url = "jdbc:snappydata:";
      String driver = "io.snappydata.jdbc.EmbeddedDriver";
      loadDriver(driver);
      try {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        Connection conn = DriverManager.getConnection(url, props);
        Log.getLogWriter().info("Obtained connection");
        testHookInstance.createTables(conn, false);
        testHookInstance.saveTableMetaDataToBB(conn);
      } catch (SQLException se) {
        throw new TestException("Got Exception while getting connection.", se);
      }*/
      testInstance.createSnappyTables();
      HydraTask_initializeTablesMetaData();
      /*
    SparkContext sc = SnappyContext.globalSparkContext();
    SnappyContext snc = new SnappyContext(sc);
    Map<String,String> prop = new HashMap<>();
    prop.put("PERSISTENT","async");
    snc.createTable("orders", "column",  prop,true);
    */
    }
  }


  /**
   * Start a snappy server. Any number of snappy servers can be started.
   */
  public void startSnappyServer(Properties props) {
    NativeCalls.getInstance().setEnvironment("SPARK_LOCAL_IP", "localhost");
    // bootProps.setProperty("log-level", "info");
    Server server = ServiceManager.getServerInstance();
    try {
      server.start(props);
      ServiceManager.getServerInstance().startNetworkServer("localhost", -1, null);
    } catch (SQLException se) {
      throw new TestException("Error starting server.", se);
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
      Connection conn = getLocatorConnection();
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
      Connection conn = getLocatorConnection();
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

  protected void waitForBarrier(int numThreads, String groupID) {
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(numThreads, groupID);
    Log.getLogWriter().info("Waiting for " + numThreads + " to meet at barrier");
    barrier.await();
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
