package io.snappydata.hydra.cdcConnector;

/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class CdcPerfSparkJob {

  public static ArrayList<String> queryList;
  public static int THREAD_COUNT;
  public static Long finalTime = 0l;

  public static long runQuery() {
    Connection conn = null;
    long timeTaken = 0l;
    try {
      System.out.println("Inside runQuery");
      Random rnd = new Random();
      long startTime;
      long endTime;
      int numItr= 200;
      System.out.println("Getting connection");
      String url = "jdbc:snappydata://dev11:1527";
      String driver = "io.snappydata.jdbc.ClientDriver";
      Class.forName(driver);


      //conn = TomcatConnectionPoolForSparkApp.getConnection();
      conn = DriverManager.getConnection(url);
     // String query = "SELECT *  FROM POSTAL_ADDRESS WHERE PSTL_ADDR_ID = ? AND CNTC_ID = ?";
     String query = "SELECT * FROM POSTAL_ADDRESS WHERE CNTC_ID = ? AND CLIENT_ID = ?";
      // String query = "SELECT * FROM POSTAL_ADDRESS WHERE CTY = ? AND ST = ?";
   //  String query = "SELECT * FROM CLAIM_STATUS WHERE PRSN_ID = ? AND CLM_ID = ?";
   //   System.out.println("The query string is " + query + " with CNTC_ID : " + CNTC_ID + " and CLIENT_ID: " + CLIENT_ID);
      PreparedStatement ps = conn.prepareStatement(query);
     /* ps.setInt(1, CNTC_ID);
      ps.setInt(2, CLIENT_ID);*/

      // warm up loop
      /*for (int i = 0; i < 100; i++) {
        ps.executeQuery();
      }*/

      // actuall query execution task
      startTime = System.currentTimeMillis();
      for(int i = 0 ; i < numItr ; i++) {
        int CLIENT_ID = rnd.nextInt(10000);
        int CNTC_ID = rnd.nextInt(10000);
        ps.setInt(1, CNTC_ID);
        ps.setInt(2, CLIENT_ID);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
          String CITY = rs.getString("CTY");
          String COUNTRY = rs.getString("CNTY");
       //   System.out.println("   " + CITY + " " + COUNTRY);
        }
       }
      endTime = System.currentTimeMillis();
      timeTaken = (endTime - startTime);///numItr;
      System.out.println("Query executed successfully and with "+numItr+" iterrations ,finished in " + timeTaken + " Time(ms)");
      System.out.println("Avg time of "+numItr+" iterration is " +timeTaken/numItr + " Time(ms)");

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

  public static void main(String[] args) throws InterruptedException {
    queryList = new ArrayList<String>();
    THREAD_COUNT = Integer.parseInt(args[0]);
    final List<Long> timeList = new CopyOnWriteArrayList<>();
    long startTime;
    long endTime;
    final CountDownLatch startBarierr = new CountDownLatch(THREAD_COUNT + 1);
    final CountDownLatch finishBarierr = new CountDownLatch(THREAD_COUNT);
    for (int i = 0; i < THREAD_COUNT; i++) {
      final int iterationIndex = i;
      new Thread(new Runnable() {
        public void run() {
          startBarierr.countDown();
          System.out.println("Thread " + iterationIndex + " started");
          try {
            startBarierr.await();
            long time = runQuery();
            long actualTime = time/200;
            timeList.add(actualTime);// a CopyOnWriteArray list to store the query execution time of each thread
            System.out.println("Time returned is " + time + " finaltime is = " + actualTime);
            System.out.println("Thread " + iterationIndex + " finished ");
            finishBarierr.countDown(); //current thread finished, send mark
          } catch (InterruptedException e) {
            throw new AssertionError("Unexpected thread interrupting");
          }
        }
      }).start();
    }
    startBarierr.countDown();
    startBarierr.await(); //await start for all thread
   // startTime = System.currentTimeMillis(); //and note time
    finishBarierr.await(); //wait each thread
  //  endTime = System.currentTimeMillis();   //note finish time*/

    //finally when all the threads have finished query execution,add all the query execution time from the list.
    for (int i = 0; i < timeList.size(); i++) {
      finalTime += timeList.get(i);
    }

    System.out.println("Avg time from timeList, taken to execute a query  with  " + THREAD_COUNT + " threads is " + (finalTime / THREAD_COUNT) + " (ms)");


  }
/*  public static void draw(int[] count) {
    double[] x = CData.GetRange(0.0D, 1.0D, 50.0D);
    double[] y = new double[x.length];
    for(int i =0; i<y.length;++i) {
      y[i] = x[i]*x[i];
    }
    Panel panel = new Panel();

  } */
}

