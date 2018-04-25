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

import hydra.Log;
import org.apache.spark.sql.catalyst.plans.logical.Except;

class cdcObject implements Runnable {
  public String path;
  private Thread t;
  private String threadName;
  private String sqlServer;
  private String dataBaseName;
  private int startRange;
  private int endRange;

  cdcObject(String name, int strNum, int endNum, String qPath, String sqlSer) {
    threadName = name;
    startRange = strNum;
    endRange = endNum;
    path = qPath;
    sqlServer = sqlSer;
    System.out.println("Creating " + threadName);
  }

  public void run() {
    System.out.println("Running " + threadName);
    try {
      Connection conn = getConnection();
      String filePath = path;
      ArrayList qArr = getQuery(filePath);
      insertData(qArr,conn);
    } catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    }
  }

  public static Connection getSnappyConnection() {
    Connection conn = null;
    String url = "jdbc:snappydata://dev11:1527";
    String driver = "io.snappydata.jdbc.ClientDriver";
    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
    } catch (Exception ex) {
      System.out.println("Caught exception in getSnappyConnection() method" + ex.getMessage());
    }
    return conn;
  }

  public Connection getConnection() {
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
      System.out.println("Caught exception in getConnection() " + e.getMessage());
    }
    return conn;
  }

  public void insertData(ArrayList<String> queryArray, Connection conn) {
    try {
      for (int i = 0; i < queryArray.size(); i++) {
        String qStr = queryArray.get(i);
        System.out.println("Query = " + qStr);
        for (int j = startRange; j <= endRange; j++) {
          PreparedStatement ps = conn.prepareStatement(qStr);
          int KEY_ID = j;
          ps.setInt(1, KEY_ID);
          ps.execute();
        }
        System.out.println("Thread " + threadName + " finished  ingesting " + (endRange - startRange) + " rows in a table");
      }
      System.out.println("FINISHED: Thread " + threadName + " finished ingestion in all the tables");
    } catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
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
    } catch (IOException io) {
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
}

public class CDCIngestionApp {

  public static void main(String args[]) {
    try {
      int sRange = Integer.parseInt(args[0]);
      int eRange = Integer.parseInt(args[1]);
      String insertQPAth = args[2];
      String sqlServerInstance = args[3];
      System.out.println("The startRange is " + sRange + " and the endRange is " + eRange);
      for (int i = 1; i <= 5; i++) {
        cdcObject obj = new cdcObject("Thread-" + i, sRange, eRange, insertQPAth+"/insert"+i+".sql", sqlServerInstance);
        obj.start();
     }
    } catch (Exception e) {
    } finally {
      System.out.println("Spark ApplicationEnd: ");
    }
  }
}
