package io.snappydata.hydra.cdcConnector;

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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class CDCPerfSparkJob {

  public static ArrayList<String> queryList;
  public static int THREAD_COUNT;
  public static Long finalTime = 0l;
  public static String hostPort;

  public static Connection getConnection() {
    Connection conn = null;
    String url = "jdbc:snappydata://" + hostPort;
    String driver = "io.snappydata.jdbc.ClientDriver";
    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
    } catch (Exception ex) {
      System.out.println("Caught exception in getConnection() method" + ex.getMessage());
    }

    return conn;
  }

  public static Connection getSqlServerConnection(String sqlServer) {
    Connection conn = null;
    try {
      System.out.println("Getting connection");
      String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
      Class.forName(driver);
      String url;
      if (sqlServer.equals("sqlServer1")) {
        url = "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433";
      } else
        url = "jdbc:sqlserver://sqlserver2-et16.copfedn1qbcz.us-west-2.rds.amazonaws.com:1435";
      String username = "sqldb";
      String password = "snappydata#msft1";
      Properties props = new Properties();
      props.put("username", username);
      props.put("password", password);
      // Connection connection ;
      System.out.println("username = " + username + " password = " + " url = " + url);
      conn = DriverManager.getConnection(url, props);

      System.out.println("Got connection" + conn.isClosed());
    } catch (Exception e) {
      System.out.println("Caught exception in getSqlServerConnection() " + e.getMessage());
    }
    return conn;
  }

  public static HashMap<List<Integer>, Map<String, Long>> runPointLookupQueries(ArrayList<String> qlist, int startRange, Integer ThreadId) {
    long timeTaken = 0l;
    long startTime;
    long endTime;
    Random rnd = new Random();
    Connection conn;
    List<Integer> threadname = new CopyOnWriteArrayList<>();
    Map<String, Long> queryTimeMap = new HashMap<>();
    HashMap<List<Integer>, Map<String, Long>> plTimeListHashMap = new HashMap<>();
    conn = getConnection();
    try {
      if (qlist.size() == 0)
        System.out.println("The queryList is empty");
      else {
        int queryPos = rnd.nextInt(qlist.size());
        System.out.println(ThreadId + " warm up query = " + qlist.get(queryPos));
        PreparedStatement ps = conn.prepareStatement(qlist.get(queryPos));
        Random rnd1 = new Random();
        int val = rnd1.nextInt(10) + startRange;

        // warm up task loop:
        for (int i = 0; i < 100; i++) {  // iterrate each query 100 times.
          ps.setInt(1, val);
          // conn.createStatement().executeQuery(qlist.get(queryPos));
          ps.executeQuery();
        }

        threadname.add(ThreadId);
        System.out.println(ThreadId + " executing query = " + qlist.get(queryPos));
        System.out.println("");
        startTime = System.currentTimeMillis();
        ResultSet rs = conn.createStatement().executeQuery(qlist.get(queryPos));
        while (rs.next()) {

        }
        endTime = System.currentTimeMillis();
        timeTaken = (endTime - startTime);
        queryTimeMap.put(qlist.get(queryPos), timeTaken);
        plTimeListHashMap.put(threadname, queryTimeMap); // Hash Map contains (threadname ->(query,queryExecutionTime))
      }
    } catch (Exception ex) {
      System.out.println("Caught Exception in runPointLooUpQueries() method" + ex.getMessage());
    }
    return plTimeListHashMap;
  }

  public static void runMixedQuery(ArrayList<String> qlist, int startRange, String serverInstance) {
    Connection sqlConn = null;
    Connection snappyConn;
    try {
      System.out.println("The hostPort is " + hostPort);
      for (int i = 0; i < qlist.size(); i++) {
        String query = qlist.get(i);
        System.out.println("The query is " + query);
        if (query.contains("SELECT")) {
          System.out.println("Query contains select so getting snappy connection");
          snappyConn = getConnection();
          Random rd = new Random();
          PreparedStatement ps = snappyConn.prepareStatement(query);
          ps.setInt(1, rd.nextInt(startRange) + startRange);
          ResultSet rs = ps.executeQuery();
          printResultSetData(rs);
          snappyConn.close();
        } else {
          System.out.println("Query contains insert/delete/update so get sqlServer connection");
          sqlConn = getSqlServerConnection(serverInstance);
          if (query.contains("UPDATE")) {
            Random rd = new Random();
            PreparedStatement ps = sqlConn.prepareStatement(query);
            System.out.println("The update query is " + query);
            ps.setInt(1, rd.nextInt(startRange) + startRange);
            ps.execute();
          } else
            sqlConn.createStatement().execute(query);
        }
      }
    } catch (Exception ex) {
      System.out.println("Exception inside runMixedQuery() method" + ex.getMessage());
    }
  }

  public static void printResultSetData(ResultSet rs) {
    try {
      while (rs.next()) {
        ResultSetMetaData md = rs.getMetaData();
        int colCnt = md.getColumnCount();
        for (int i = 1; i <= colCnt; i++) {
          String colVal = rs.getString(i);
          String colName = md.getColumnName(i);
          System.out.println("The ColName = " + colName + " ColVal = " + colVal);
        }
      }
    } catch (SQLException ex) {
      System.out.println("Caught SQL exception in printResultSetData " + ex.getMessage());
    }
  }


  public static void runBulkDelete(ArrayList<String> qlist, int startRange, String serverInstance) {
    Connection conn = null;
    try {
      conn = getSqlServerConnection(serverInstance);
      System.out.println("The start Range is " + startRange);
      for (int i = 0; i < qlist.size(); i++) {
        System.out.println("The query is " + qlist.get(i));
        PreparedStatement ps = conn.prepareStatement(qlist.get(i));
        //  ps.setInt(1, (startRange - 1));
        ps.setInt(1, 1);
        ps.execute();
        System.out.println("successfully executed the query");
      }
    } catch (Exception ex) {
      System.out.println("Exception inside runBulkDelete() method" + ex.getMessage());
    } finally {
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static long runScanQuery() {
    Connection conn = null;
    long timeTaken = 0l;
    try {
      System.out.println("Inside runQuery");
      Random rnd = new Random();
      long startTime;
      long endTime;
      int numItr = 200;

      conn = getConnection();
      String query = "SELECT * FROM POSTAL_ADDRESS WHERE CNTC_ID = ? AND CLIENT_ID = ?";
      PreparedStatement ps = conn.prepareStatement(query);

      // warm up loop
      for (int i = 0; i < 100; i++) {
        ps.executeQuery();
      }
      // actuall query execution task
      startTime = System.currentTimeMillis();
      for (int i = 0; i < numItr; i++) {
        int CLIENT_ID = rnd.nextInt(10000);
        int CNTC_ID = rnd.nextInt(10000);
        ps.setInt(1, CNTC_ID);
        ps.setInt(2, CLIENT_ID);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
          String CITY = rs.getString("cty");
          String COUNTRY = rs.getString("cnty");
        }
      }
      endTime = System.currentTimeMillis();
      timeTaken = (endTime - startTime);///numItr;
      System.out.println("Query executed successfully and with " + numItr + " iterrations ,finished in " + timeTaken + " Time(ms)");
      System.out.println("Avg time of " + numItr + " iterration is " + timeTaken / numItr + " Time(ms)");

    } catch (Exception e) {
      System.out.println("Caught exception " + e.getMessage());
    } finally {
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return timeTaken;
  }

  public static ArrayList getQueryArr(String fileName) {
    System.out.println("QueryFile Name = " + fileName);
    ArrayList<String> queries = new ArrayList<String>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line;
      while ((line = br.readLine()) != null) {
        String[] splitData = line.split(";");
        for (int i = 0; i < splitData.length; i++) {
          if (!(splitData[i] == null) || !(splitData[i].length() == 0)) {
            String qry = splitData[i];
            queries.add(qry);
          }
        }
      }
      br.close();
    } catch (FileNotFoundException e) {
    } catch (IOException io) {
    }
    System.out.println("The querylist consists of " + queries.size() + " queries");
    return queries;
  }


  public void runConcurrencyTestJob(int threadCnt, String filePath, String hostP, Boolean isScanQuery, Boolean isBulkDelete, Boolean isPointLookUp, Boolean isMixedQuery, int startRange, String sqlServerInst) {
    System.out.println("Inside the actual task");
    THREAD_COUNT = threadCnt;
    queryList = getQueryArr(filePath);
    hostPort = hostP;
    final List<Long> timeList = new CopyOnWriteArrayList<>();
    final List<HashMap<List<Integer>, Map<String, Long>>> plQryTimeList = new CopyOnWriteArrayList<>();
    final CountDownLatch startBarierr = new CountDownLatch(THREAD_COUNT + 1);
    final CountDownLatch finishBarierr = new CountDownLatch(THREAD_COUNT);
    try {
      for (int i = 0; i < THREAD_COUNT; i++) {
        final int iterationIndex = i;
        new Thread(new Runnable() {
          public void run() {
            startBarierr.countDown();
            System.out.println("Thread " + iterationIndex + " started");
            try {
              startBarierr.await();
              if (isMixedQuery) {
                runMixedQuery(queryList, startRange, sqlServerInst);
              }
              if (isBulkDelete) {
                runBulkDelete(queryList, startRange, sqlServerInst);
              }
              if (isScanQuery) {
                long scanTime = runScanQuery();
                long actualTime = scanTime / 200;
                timeList.add(actualTime);// a CopyOnWriteArray list to store the query execution time of each thread
                System.out.println("Time returned is " + scanTime + " finaltime is = " + actualTime);
                System.out.println("Thread " + iterationIndex + " finished ");
              }
              if (isPointLookUp)
                plQryTimeList.add(runPointLookupQueries(queryList, startRange, iterationIndex));

              finishBarierr.countDown(); //current thread finished, send mark
            } catch (InterruptedException e) {
              throw new AssertionError("Unexpected thread interrupting");
            }
          }
        }).start();
      }
      startBarierr.countDown();
      startBarierr.await(); //await start for all thread
      finishBarierr.await(); //wait each thread
    } catch (Exception ex) {

    }
    //finally when all the threads have finished executing query,add all the query execution time from the list.
    for (int j = 0; j < plQryTimeList.size(); j++) {
      HashMap<List<Integer>, Map<String, Long>> tempHashMap = plQryTimeList.get(j);
      for (Map.Entry<List<Integer>, Map<String, Long>> entry : tempHashMap.entrySet()) {
        System.out.println("");
        System.out.println("The thread id is " + entry.getKey());
        Map<String, Long> queryTimeMap = entry.getValue();
        for (Map.Entry<String, Long> val : queryTimeMap.entrySet()) {
          long time = val.getValue();
          System.out.println("Query = " + val.getKey() + " took = " + val.getValue() + " ms to execute");
        }
      }
    }
    for (int i = 0; i < timeList.size(); i++) {
      finalTime += timeList.get(i);
    }
    System.out.println("Avg time from timeList, taken to execute a query  with  " + THREAD_COUNT + " threads is " + (finalTime / THREAD_COUNT) + " (ms)");
    System.out.println("Spark ApplicationEnd: ");
  }


  public static void main(String[] args) throws InterruptedException {
    int threadCnt = Integer.parseInt(args[0]);
    String queryPath = args[1];
    Boolean isScanQuery = Boolean.parseBoolean(args[2]);
    String hostName = args[3];
    Boolean isbulkDelete = Boolean.parseBoolean(args[4]);
    Boolean ispointLookup = Boolean.parseBoolean(args[5]);
    Boolean isMixedQuery = Boolean.parseBoolean(args[6]);
    int startRange = Integer.parseInt(args[7]);
    String sqlServerInst = args[8];

    CDCPerfSparkJob cdcPerfSparkJob = new CDCPerfSparkJob();
    cdcPerfSparkJob.runConcurrencyTestJob(threadCnt, queryPath, hostName, isScanQuery, isbulkDelete, ispointLookup, isMixedQuery, startRange, sqlServerInst);
  }
}

