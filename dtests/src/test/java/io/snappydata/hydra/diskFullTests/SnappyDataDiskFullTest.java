package io.snappydata.hydra.diskFullTests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import hydra.Log;
import io.snappydata.hydra.cdcConnector.SnappyCDCPrms;
import io.snappydata.hydra.cdcConnector.SnappyCDCTest;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;
import org.apache.commons.collections.map.HashedMap;

public class SnappyDataDiskFullTest extends SnappyTest {

  public static SnappyDataDiskFullTest snappyDataDiskFullTest;
  protected static SnappyCDCTest snappyCDCTest;

  public SnappyDataDiskFullTest() {
  }

  public ArrayList getQueryList(String fileName) {
    ArrayList queryList = new ArrayList<String>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] splitData = line.split(";");
        for (int i = 0; i < splitData.length; i++) {
          if (!(splitData[i] == null) || !(splitData[i].length() == 0)) {
            String qry = splitData[i];
            System.out.println("The query is " + qry);
            queryList.add(qry);
          }
        }
      }
      br.close();

    } catch (FileNotFoundException e) {
      System.out.println("Caught FileNotFoundException in getQuery() " + e.getMessage());
    } catch (IOException io) {
      System.out.println("Caught IOException in getQuery() " + io.getMessage());
    }
    return queryList;
  }

  public static void HydraTask_executeOps() {
    if (snappyDataDiskFullTest == null) {
      snappyDataDiskFullTest = new SnappyDataDiskFullTest();
    }
    snappyDataDiskFullTest.executeOps();
  }

  public void executeOps() {
    Log.getLogWriter().info("Inside executeOps");
    String queryFileName = SnappyCDCPrms.getDataLocation();
    int numRecords = SnappyDataDiskFullPrms.getNumInserts();
    int totalRecords1 = 0;
    int totalRecords2 = 0;
    String query = null;
    ArrayList<String> queryList = getQueryList(queryFileName);
    Map<String, Integer> tableValueMap = new HashedMap();
    Connection conn = null;
    try {
      List<String> endpoints = validateLocatorEndpointData();
      String url = "jdbc:snappydata://" + endpoints.get(0);
      String driver = "io.snappydata.jdbc.ClientDriver";
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
      while (true) {
        for (int i = 0; i <= queryList.size() - 1; i++) {
          query = queryList.get(i);
          Log.getLogWriter().info("Starting execution of " + query + " query");
          conn.createStatement().execute(query);
          if (query.startsWith("INSERT")) {
            String tableName = query.split(" ")[2];
            if (tableName.equalsIgnoreCase("AGREEMENT")) {
              totalRecords1 += numRecords;
              tableValueMap.put(tableName, totalRecords1);
            } else {
              totalRecords2 += numRecords;
              tableValueMap.put(tableName, totalRecords2);
            }
            Log.getLogWriter().info("Successfully inserted records for table = " + tableName);
          } else {
            Log.getLogWriter().info("Successfully deleted records.");
          }
        }
      }
    } catch (SQLException ex) {
      try {
        //Reexecute the query list after exception.
        for (int i = 0; i <= queryList.size() - 1; i++) {
          String queryStr = queryList.get(i);
          conn.createStatement().execute(queryStr);
        }
      } catch (SQLException ex1) {
        Log.getLogWriter().info("Caught exception after successful inserts \n" + ex1.getMessage());
      }
    } catch (Exception ex) {
      throw new TestException("Caught exception " + ex.getMessage());
    } finally {
      SnappyBB.getBB().getSharedMap().put("TABLE_VALUE_MAP", tableValueMap);

    }
  }

  public static void HydraTask_executeCleanUpFiles() {
    if (snappyDataDiskFullTest == null) {
      snappyDataDiskFullTest = new SnappyDataDiskFullTest();
    }
    snappyDataDiskFullTest.executeCleanUpFiles();
  }

  public void executeCleanUpFiles() {
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterRestart.log";
      File logFile = new File(dest);
      Map<String, Integer> tableValueMap = new HashedMap();
      String tableName = "";
      int numRecords = SnappyDataDiskFullPrms.getNumInserts();
      int expectedCnt = 0;
      String cmd = "rm -rf " + snappyTest.getCurrentDirPath() + "/randomFileForDiskFull_*";
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
      snappyTest.executeProcess(pb, logFile);
      Log.getLogWriter().info("Restarting the cluster after cleanup");
      snappyCDCTest.stopCluster(snappyPath, logFile);
      //wait for 1 mins
      Thread.sleep(60000);
      snappyCDCTest.startCluster(snappyPath, logFile, false);
      Log.getLogWriter().info("Verify the total inserts");
      tableValueMap = (HashedMap) SnappyBB.getBB().getSharedMap().get("TABLE_VALUE_MAP");
      for (Map.Entry<String, Integer> entry : tableValueMap.entrySet()) {
        tableName = entry.getKey();
        expectedCnt = entry.getValue();
        Connection conn = SnappyTest.getLocatorConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
        while (rs.next()) {
          int actualCnt = rs.getInt(1);
          if (actualCnt >= expectedCnt && actualCnt <= (expectedCnt + numRecords)) {
            Log.getLogWriter().info("The data inserted is correct");
            Log.getLogWriter().info("Expected count = " + expectedCnt + " actual count = " + actualCnt);
          } else
            Log.getLogWriter().info("The data in restarted cluster is less \n" + " Expected count  " + expectedCnt + " Actually Found " + actualCnt);
        }
      }
    } catch (Exception ex) {
      throw new TestException("Caught exception during cleanup " + ex.getMessage());
    }
  }

  public static void HydraTask_executeFallocate() {
    if (snappyDataDiskFullTest == null) {
      snappyDataDiskFullTest = new SnappyDataDiskFullTest();
    }
    snappyDataDiskFullTest.executeFallocate();
  }

  public void executeFallocate() {
    File logFile = null;
    try {
      String fileSize = SnappyDataDiskFullPrms.getFileSize();
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "executeFallocate.log";
      String diskFullFileName = log.getCanonicalPath() + File.separator + "randomFileForDiskFull_" + System.currentTimeMillis();
      File randomFileForDiskFull = new File(diskFullFileName);
      logFile = new File(dest);
      Log.getLogWriter().info("Creating a randomFile.txt with " + fileSize + " size");
      String cmd = "fallocate -l " + fileSize + " " + randomFileForDiskFull;
      Log.getLogWriter().info("The command to be executed is " + cmd);
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
      snappyTest.executeProcess(pb, logFile);
    } catch (IOException io) {
      if (io.getMessage().contains("No space left on device")) {
        Log.getLogWriter().info("Caught No disk Space error in executeFallocate");
      } else
        throw new TestException("IOException in executeFallocate() " + io.getMessage());
    } catch (Exception ex) {
      throw new TestException("CAught exception " + ex.getMessage());
    }
  }

}
