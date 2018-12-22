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
package io.snappydata.hydra.cluster;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import hydra.TestConfig;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static hydra.Prms.totalTaskTimeSec;

public class SnappyConcurrencyTest extends SnappyTest {

  public static long totalTaskTime = TestConfig.tab().longAt(totalTaskTimeSec);
  public static long warmUpTimeSec = TestConfig.tasktab().longAt(SnappyPrms.warmUpTimeSec, TestConfig.tab().
      longAt(SnappyPrms.warmUpTimeSec, 300));

  public static boolean isTPCHSchema = TestConfig.tab().booleanAt(SnappyPrms.isTPCHSchema, false);  //default to false

  public static void runPointLookUpQueries() throws SQLException {
    Vector<String> queryVect = SnappyPrms.getPointLookUpQueryList();
    String query = null;
    Connection conn = getLocatorConnection();
    ResultSet rs;
    long startTime = System.currentTimeMillis();
    long endTime = startTime + warmUpTimeSec * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing pointLookUp query:" + query, se);
      }
    }
    endTime = startTime + totalTaskTime * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numPointLookUpQueriesExecuted);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing pointLookUp query:" + query, se);
      }
    }
    /*StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> queryResult = ResultSetHelper.asList(rs, sti, false);
    Log.getLogWriter().info("SS - Result for query : " + query + "\n" + queryResult.toString());*/
    closeConnection(conn);
  }

  public static void runAnalyticalQueries() throws SQLException {
    Connection conn = getLocatorConnection();
    Vector<String> queryVect = SnappyPrms.getAnalyticalQueryList();
    String query = null;
    ResultSet rs;
    if (isTPCHSchema) {
      query = "create or replace temporary view revenue as select  l_suppkey as supplier_no, " +
          "sum(l_extendedprice * (1 - l_discount)) as total_revenue from LINEITEM where l_shipdate >= '1993-02-01'" +
          " and l_shipdate <  add_months('1996-01-01',3) group by l_suppkey";
      conn.createStatement().executeUpdate(query);
    }
    long startTime = System.currentTimeMillis();
    long endTime = startTime + warmUpTimeSec * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing Analytical query:" + query, se);
      }
    }
    startTime = System.currentTimeMillis();
    endTime = startTime + totalTaskTime * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
        long queryExecutionEndTime = System.currentTimeMillis();
        long queryExecutionTime = queryExecutionEndTime - startTime;
        //Log.getLogWriter().info("SS - queryExecutionTime for query:  " + queryNum + ":" + query + " is: " + queryExecutionTime);
        SnappyBB.getBB().getSharedMap().put(queryNum + "_" + query + "_" + System.currentTimeMillis(), queryExecutionTime);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numAggregationQueriesExecuted);
      } catch (SQLException se) {
        throw new TestException("Got exception while executing Analytical query:" + query, se);
      }
    }
    /*StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> queryResult = ResultSetHelper.asList(rs, sti, false);
    Log.getLogWriter().info("SS - Result for query : " + query + "\n" + queryResult.toString());*/
    closeConnection(conn);
  }

  public static void validateNumQueriesExecuted() throws SQLException {
    int numQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numQueriesExecuted);
    int numpointLookUpQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB
        .numPointLookUpQueriesExecuted);
    int numAggregationQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numAggregationQueriesExecuted);
    Log.getLogWriter().info("Total number of queries executed : " + numQueriesExecuted);
    Log.getLogWriter().info("Total number of pointLookUp queries executed : " +
        numpointLookUpQueriesExecuted);
    Log.getLogWriter().info("Total number of analytical queries executed : " + numAggregationQueriesExecuted);
    Vector<String> queryVect = SnappyPrms.getAnalyticalQueryList();
    FileOutputStream latencyStream = null;
    try {
      latencyStream = new FileOutputStream(new File("LatencyStats.csv"));
      PrintStream latencyPrintStream = new PrintStream(latencyStream);
      latencyPrintStream.println("Query, Min, Max, Avg, numExecutions");

      for (int i = 0; i < queryVect.size(); i++) {
        String queryNum = "Q" + i;
        writeQueryExecutionTimingsData(queryNum, i + "_", latencyPrintStream);
      }
      latencyPrintStream.close();
      latencyStream.close();
    } catch (FileNotFoundException fne) {
      String s = "Unable to find file: LatencyStats.csv";
      throw new TestException(s);
    } catch (IOException e) {
      throw new TestException("IOException occurred while closing file stream... \nError Message:" + e.getMessage());
    }
  }

  protected static void writeQueryExecutionTimingsData(String fileName, String queryNum, PrintStream latencyPrintStream) {
    File currentDir = new File(".");
    String filePath = currentDir + fileName;
    File file = new File(filePath);
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        throw new TestException("IOException occurred while creating file " +
            file + "\nError Message:" + e.getMessage());
      }
    }

    ArrayList<Long> timings = new ArrayList<>();
    timings = getQueryExecutionTimingsData(queryNum, timings);
    if (timings.size() == 0) {
      String s = "No data found for writing to " + fileName + " file under test directory";
      Log.getLogWriter().info("No data found for writing to " + fileName + " file under test directory");
      return;
    }
    long max = Collections.max(timings);
    long min = Collections.min(timings);
    long avg;
    long sum = 0;
    for (int i = 0; i < timings.size(); i++) {
      sum += timings.get(i);
    }
    avg = sum / timings.size();
    queryNum = queryNum.substring(0, queryNum.lastIndexOf("_"));
    String string = String.format(queryNum + ", " + min + ", " + max + ", " + avg + ", " + timings.size());
    latencyPrintStream.println(string);
    for (Long s : timings) {
      snappyTest.writeToFile(s.toString(), file, true);
    }
    snappyTest.writeToFile("Min: " + min, file, true);
    snappyTest.writeToFile("Max: " + max, file, true);
    snappyTest.writeToFile("Average: " + avg, file, true);
  }

  protected static ArrayList<Long> getQueryExecutionTimingsData(String userKey, ArrayList<Long>
      timings) {
    Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
    for (String key : keys) {
      if (key.startsWith(userKey)) {
        Long value = (Long) SnappyBB.getBB().getSharedMap().get(key);
        timings.add(value);
      }
    }
    Log.getLogWriter().info("ArrayList contains : " + timings.toString());
    return timings;
  }

  public static synchronized void HydraTask_executeDeployJarQuery() throws SQLException {
    Connection conn = getLocatorConnection();
    String query = null;
    String userJarPath = snappyTest.getUserAppJarLocation(SnappyPrms.getUserAppJar(), jarPath);
    try {
      query = "deploy jar jdbcStream '" + userJarPath + "'";
      Log.getLogWriter().info("Deploy jar query is: " + query);
      conn.createStatement().executeUpdate(query);

    } catch (SQLException se) {
      throw new TestException("Got exception while executing deploy jar query:" + query, se);
    }
    closeConnection(conn);
  }
}
