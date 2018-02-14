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

class cdcObject implements Runnable {
  private Thread t;
  private String threadName;

  cdcObject( String name) {
    threadName = name;
    System.out.println("Creating " +  threadName );
  }
  public void run() {
    //Implement switch case later on instead of if else.
    System.out.println("Running " +  threadName );
     try {

      switch (threadName) {
        case "Thread-1":
          String f1 = "/home/supriya/snappy/snappydata/dtests/src/resources/scripts/cdcConnector/insert1.sql";
          Connection conn1 = null;
          ArrayList qArr = this.getQuery(f1);
          try {
            conn1 = this.getConnection();
            this.insertData(qArr, conn1);
          }
          catch (Exception e) {
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
          String f2 = "/home/supriya/snappy/snappydata/dtests/src/resources/scripts/cdcConnector/insert2.sql";
          ArrayList qArr2 = this.getQuery(f2);
          Connection conn2 = null;
          try {
            conn2 = this.getConnection();
            this.insertData(qArr2, conn2);
          }
          catch (Exception e) {
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
          String f3 = "/home/supriya/snappy/snappydata/dtests/src/resources/scripts/cdcConnector/insert3.sql";
          ArrayList qArr3 = this.getQuery(f3);
          Connection conn3 = null;
          try {
            conn3 = this.getConnection();
            this.insertData(qArr3, conn3);
          }
          catch (Exception e) {
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
          String f4 = "/home/supriya/snappy/snappydata/dtests/src/resources/scripts/cdcConnector/insert4sql";
          ArrayList qArr4 = this.getQuery(f4);
          Connection conn4=null;
          try {
            conn4 = this.getConnection();
            this.insertData(qArr4, conn4);
          }
          catch (Exception e) {
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
          String f5 = "/home/supriya/snappy/snappydata/dtests/src/resources/scripts/cdcConnector/insert5.sql";
          ArrayList qArr5 = this.getQuery(f5);
          Connection conn5 = null;
          try {
            conn5 = this.getConnection();
            this.insertData(qArr5, conn5);
          }
          catch (Exception e) {
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
    }
       catch (Exception e) {
        System.out.println("Caught exception " + e.getMessage());
      }
    }

  public Connection getConnection(){
    Connection conn = null;
    try {
      System.out.println("Getting connection");
      String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
      Class.forName(driver);
      String url = "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433";
      String username = "sqldb";
      String password = "snappydata#msft1";
      Properties props = new Properties();
      props.put("username", username);
      props.put("password", password);
      // Connection connection ;
      conn = DriverManager.getConnection(url, props);

      System.out.println("Got connection" + conn.isClosed());

    }
    catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    }
    return conn;
  }

  public void insertData(ArrayList<String> queryArray,Connection conn){
    Random rnd = new Random();
    try {
      for (int i = 0; i < queryArray.size(); i++) {
        String qStr = queryArray.get(i);
        System.out.println("Query = " + qStr);
        for (int j = 0; j < 20; j++) {
          PreparedStatement ps = conn.prepareStatement(qStr);
          int KEY_ID = rnd.nextInt(10000);
          ps.setInt(1, KEY_ID);
          ps.execute();
        }
        System.out.println("RunnableDemo: " + threadName);
      }
    }
    catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    }
  }
  public void updateData(){
    for(int i=1; i<=100; i++)
    {

      System.out.println("RunnableDemo: " + threadName + ", " + i);
    }
  }
  public void printHundredMore(){
    for(int i=100; i<=200; i++)
    {

      System.out.println("RunnableDemo: " + threadName + ", " + i);
    }
  }

  public ArrayList getQuery(String fileName){
    ArrayList queryList = new ArrayList<String>();
    try {

     // fileName = "/home/supriya/snappy/snappydata/dtests/src/test/java/io/snappydata/hydra/cdcConnector/custInsert.sql";
      //
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

  public void start () {
    System.out.println("Starting " +  threadName );
    if (t == null) {
      t = new Thread (this);
      t.start ();
      t.setName(threadName);
    }
  }
}
public class cdcIngestionApp {
  public static void main(String args[]) {
    for(int i=1 ; i <= 5 ; i++){
      cdcObject obj = new cdcObject("Thread-"+i);
      obj.start();
    }

  }
}
