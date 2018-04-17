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

      switch (threadName) {
        case "Thread-1":
          String f1 = path + "/insert1.sql";
          String upQryF1 = "";
          Connection conn1 = null;
          ArrayList qArr = this.getQuery(f1);
          ArrayList updQArr1 = this.getQuery(upQryF1);
          try {
           conn1 = this.getConnection();
           // conn1 = this.getSnappyConnection();
            this.insertData(qArr, conn1);
            this.updateData(updQArr1, conn1);
          } catch (Exception e) {
            System.out.println("Caught exception " + e.getMessage());
          } finally {
            try {
              conn1.close();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          }
          break;
        case "Thread-2":
          String f2 = path + "/insert2.sql";
          String upQryF2 = "";
          ArrayList qArr2 = this.getQuery(f2);
          ArrayList updQArr2 = this.getQuery(upQryF2);
          Connection conn2 = null;
          try {
            conn2 = this.getConnection();
            this.insertData(qArr2, conn2);
            this.updateData(updQArr2, conn2);
          } catch (Exception e) {
            System.out.println("Caught exception " + e.getMessage());
          } finally {
            try {
              conn2.close();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          }
          break;
        case "Thread-3":
          String f3 = path + "/insert3.sql";
          String upQryF3 = "";
          ArrayList qArr3 = this.getQuery(f3);
          ArrayList updQArr3 = this.getQuery(upQryF3);
          Connection conn3 = null;
          try {
            conn3 = this.getConnection();
            this.insertData(qArr3, conn3);
            this.updateData(updQArr3, conn3);
          } catch (Exception e) {
            System.out.println("Caught exception " + e.getMessage());
          } finally {
            try {
              conn3.close();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          }
          break;
        case "Thread-4":
          String f4 = path + "/insert4.sql";
          String upQryF4 = "";
          ArrayList qArr4 = this.getQuery(f4);
          ArrayList updQArr4 = this.getQuery(upQryF4);
          Connection conn4 = null;
          try {
            conn4 = this.getConnection();
            this.insertData(qArr4, conn4);
            this.updateData(updQArr4, conn4);
          } catch (Exception e) {
            System.out.println("Caught exception " + e.getMessage());
          } finally {
            try {
              conn4.close();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          }
          break;
        case "Thread-5":
          String f5 = path + "/insert5.sql";
          String upQryF5 = "";
          ArrayList qArr5 = this.getQuery(f5);
          ArrayList updQArr5 = this.getQuery(upQryF5);
          Connection conn5 = null;
          try {
            conn5 = this.getConnection();
            this.insertData(qArr5, conn5);
            this.updateData(updQArr5, conn5);
          } catch (Exception e) {
            System.out.println("Caught exception " + e.getMessage());
          } finally {
            try {
              conn5.close();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          }
          break;
        default:
          System.out.println("Invalid Thread name = " + threadName);
      }
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
        cdcObject obj = new cdcObject("Thread-" + i, sRange, eRange, insertQPAth, sqlServerInstance);
        obj.start();
      }
    } catch (Exception e) {
    } finally {
      System.out.println("Spark ApplicationEnd: ");
    }
  }
}
