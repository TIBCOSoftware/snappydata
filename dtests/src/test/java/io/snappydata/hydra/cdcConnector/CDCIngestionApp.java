package io.snappydata.hydra.cdcConnector;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import hydra.Log;
import org.apache.spark.sql.catalyst.plans.logical.Except;

public class CDCIngestionApp implements Runnable {
  public String filePath;
  private Thread t;
  private String threadName;
  private String sqlServer;
  private int startRange;
  private int endRange;
  private String endPoint;
  private boolean isSecurityEnabled;

  public CDCIngestionApp(int sRange, int eRange, int i, String path, String sqlServerInst, String hostName, boolean isSecurity){
    threadName = "Thread-" + i;
    startRange = sRange;
    endRange = eRange;
    filePath = path + "/insert" + i + ".sql";
    sqlServer = sqlServerInst;
    endPoint = hostName;
    isSecurityEnabled = isSecurity;
  }

  public void run() {
    System.out.println("Running " + threadName);
    try {
      Connection conn;
      if (sqlServer.isEmpty())
        conn = getSnappyConnection();
      else
        conn = getSqlServerConnection();
      String path = filePath;
      ArrayList qArr = getQuery(path);
      insertData(qArr, conn);
    } catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    }
  }

  public Connection getSnappyConnection() {
    Connection conn = null;
    String url = "jdbc:snappydata://" + endPoint;
    String driver = "io.snappydata.jdbc.ClientDriver";
    Properties props = new Properties();
    if(isSecurityEnabled){
      props.setProperty("user","gemfire");
      props.setProperty("password","gemfire");
    }
   try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url,props);
    } catch (Exception ex) {
      System.out.println("Caught exception in getSnappyConnection() method" + ex.getMessage());
    }
    return conn;
  }

  public Connection getSqlServerConnection() {
    Connection conn = null;
    try {
      System.out.println("Getting connection");
      String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
      Class.forName(driver);
      String url;
      if (sqlServer.equals("sqlServer1")) {
        url = "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433";
      } else
        url = "jdbc:sqlserver://sqlent2.eastus.cloudapp.azure.com:1434";
      String username = "sqldb";
      String password = "snappydata#msft1";
      Properties props = new Properties();
      props.put("username", username);
      props.put("password", password);
      // Connection connection ;
      conn = DriverManager.getConnection(url, props);

      System.out.println("Got connection" + conn.isClosed());

    } catch (Exception e) {
      System.out.println("Caught exception in getSqlServerConnection() " + e.getMessage());
    }
    return conn;
  }

  public void insertData(ArrayList<String> queryArray, Connection conn) {
    PreparedStatement ps = null;
    try {
      final int batchSize = 1000;
      int count = 0;
      for (int i = 0; i < queryArray.size(); i++) {
        String qStr = queryArray.get(i);
        System.out.println("Query = " + qStr);
        System.out.println("The startRange = " + startRange + " the endRange = " + endRange);
        if (qStr.contains("PUT INTO")) {
          for (int j = startRange; j <= endRange; j++) {
            String newStr;
            if (qStr.contains("?"))
              newStr = qStr.replace("?", Integer.toString(j));
            else
              newStr = qStr;
            System.out.println("The new query String is " + newStr);
            conn.createStatement().execute(newStr);
            updateData(queryArray,conn);
          }
        } else {
          ps = conn.prepareStatement(qStr);
          for (int j = startRange; j <= endRange; j++) {
            int KEY_ID = j;
            ps.setInt(1, KEY_ID);
            ps.setInt(1, j);
            ps.addBatch();
            if (++count % batchSize == 0) {
              ps.executeBatch();
            }
          }
          System.out.println("Thread " + threadName + " finished  ingesting " + (endRange - startRange) + " rows in a table");
        }
      }
      System.out.println("FINISHED: Thread " + threadName + " finished ingestion in all the tables");
    } catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    } finally {
      if (ps != null) try {
        ps.close();
      } catch (SQLException ex) {
      }
      if (conn != null) try {
        conn.close();
      } catch (SQLException ex) {
      }
    }
  }

  public void updateData(ArrayList<String> queryArray, Connection conn) {
    try {
      for (int i = 0; i < queryArray.size(); i++) {
        String qStr = queryArray.get(i);
        System.out.println("Query = " + qStr);
        Random rnd = new Random();
        PreparedStatement ps = conn.prepareStatement(qStr);
        int updateKEY_ID = rnd.nextInt(endRange);
        ps.setInt(1, updateKEY_ID);
        ps.execute();
      }
    } catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    }
  }

  public ArrayList getQuery(String fileName) {
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
      System.out.println("Caught exception in getQuery() " + e.getMessage());
    } catch (IOException io) {
      System.out.println("Caught exception in getQuery() " + io.getMessage());
    }
    return queryList;
  }

  public void start() {
    System.out.println("Starting " + threadName);
    try {
      if (t == null) {
        t = new Thread(this);
        t.start();
        t.setName(threadName);
        t.join();
      }
    } catch (Exception ex) {
    }
  }

  public static void runIngestionApp(int sRange, int eRange, int thnCnt, String path, String sqlServerInst, String hostName, boolean isSecurity) {
    ExecutorService executor = Executors.newFixedThreadPool(thnCnt);
    for (int i = 1; i <= thnCnt; i++) {
      String threadName = "Thread-" + i;
      System.out.println("Creating " + threadName);
      executor.execute(new CDCIngestionApp(sRange, eRange, i,path, sqlServerInst, hostName, isSecurity));
    }
    executor.shutdown();
    try {
      executor.awaitTermination(3600, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      System.out.println("Got Exception while waiting for all threads to complete populate" +
          " tasks");
    }
  }

  public static void main(String args[]) {
    try {
      int sRange = Integer.parseInt(args[0]);
      int eRange = Integer.parseInt(args[1]);
      int threadCnt = Integer.parseInt(args[2]);
      String insertQPAth = args[3];
      String sqlServerInstance = args[4];
      String hostname = args[5];
      System.out.println("The startRange is " + sRange + " and the endRange is " + eRange);
      runIngestionApp(sRange, eRange, threadCnt, insertQPAth, sqlServerInstance, hostname, false);
    } catch (Exception e) {
      System.out.println("Caught exception in main " + e.getMessage());
    } finally {
      System.out.println("Spark ApplicationEnd: ");
    }
  }
}
