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
import hydra.HydraVector;
import hydra.Log;
import hydra.TestConfig;
import org.apache.commons.lang.StringUtils;
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
  public static boolean isStabilityTest = TestConfig.tab().booleanAt(SnappyPrms.isStabilityTest, false);  //default to false

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
    if (isStabilityTest) {
      query = "set snappydata.sql.hashAggregateSize=-1";
      conn.createStatement().executeUpdate(query);
      /*query = "set spark.driver.maxResultSize=6g";
      conn.createStatement().executeUpdate(query);*/
    }
    long startTime = System.currentTimeMillis();
    long endTime = startTime + warmUpTimeSec * 1000;
    while (endTime > System.currentTimeMillis()) {
      try {
        int queryNum = new Random().nextInt(queryVect.size());
        query = queryVect.elementAt(queryNum);
        rs = conn.createStatement().executeQuery(query);
        while (rs.next()) {
        }
        long queryExecutionEndTime = System.currentTimeMillis();
        long queryExecutionTime = queryExecutionEndTime - startTime;
        if (isStabilityTest) {
          Log.getLogWriter().info("QueryExecutionTime for query:  " + queryNum + ":" + query + " is: " + queryExecutionTime / 1000 + " secs");
        }
      } catch (SQLException se) {
        if (isStabilityTest && se.getMessage().contains("java.util.concurrent.TimeoutException: Futures timed out after"))
          Log.getLogWriter().info("Got exception while executing Analytical query:" + query, se);
        else
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
        while (rs.next()) {
        }
        long queryExecutionEndTime = System.currentTimeMillis();
        long queryExecutionTime = queryExecutionEndTime - startTime;
        if (isStabilityTest) {
          Log.getLogWriter().info("QueryExecutionTime for query:  " + queryNum + ":" + query + " is: " + queryExecutionTime / 1000 + " secs");
        }
        SnappyBB.getBB().getSharedMap().put(queryNum + "_" + query + "_" + System.currentTimeMillis(), queryExecutionTime);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
        SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numAggregationQueriesExecuted);
      } catch (SQLException se) {
        if (isStabilityTest && se.getMessage().contains("java.util.concurrent.TimeoutException: Futures timed out after"))
          Log.getLogWriter().info("Got exception while executing Analytical query:" + query, se);
        else
          throw new TestException("Got exception while executing Analytical query:" + query, se);
      }
    }
    /*StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> queryResult = ResultSetHelper.asList(rs, sti, false);
    Log.getLogWriter().info("SS - Result for query : " + query + "\n" + queryResult.toString());*/
    closeConnection(conn);
  }

  public static void createAndLoadExternalTablesUsingParquet(Vector externalTableNames, Vector dataPathList, Connection conn) throws SQLException {
    String query;
    for (int k = 0; k < externalTableNames.size(); k++) {
      String externalTableName = (String) externalTableNames.elementAt(k);
      String dataPath = (String) dataPathList.elementAt(k);
      query = "drop table if exists " + externalTableName;
      conn.createStatement().executeUpdate(query);
      query = "CREATE EXTERNAL TABLE " + externalTableName + " USING parquet OPTIONS(path '" + dataPath + "')";
      conn.createStatement().executeUpdate(query);
    }
  }

  public static void createAndLoadExternalTablesUsingCSV(Vector externalTableNames, Vector dataPathList, Connection conn) throws SQLException {
    String query;
    for (int k = 0; k < externalTableNames.size(); k++) {
      String externalTableName = (String) externalTableNames.elementAt(k);
      String dataPath = (String) dataPathList.elementAt(k);
      query = "drop table if exists " + externalTableName;
      conn.createStatement().executeUpdate(query);
      query = "CREATE EXTERNAL TABLE " + externalTableName + " USING csv OPTIONS(path '" + dataPath + "' , header 'true', inferSchema 'true' )";
      conn.createStatement().executeUpdate(query);
    }
  }

  public static void createAndLoadTables(Vector externalTableNames, Vector tableNames, Vector tableTypeList, Connection conn) throws SQLException {
    String query;
    for (int k = 0; k < tableNames.size(); k++) {
      String tableName = (String) tableNames.elementAt(k);
      String externalTableName = (String) externalTableNames.elementAt(k);
      String tableType = (String) tableTypeList.elementAt(k);
      query = "drop table if exists " + tableName;
      conn.createStatement().executeUpdate(query);
      Vector options = SnappyPrms.getTableOptions();
      if (options.isEmpty())
        query = "CREATE TABLE " + tableName + " USING " + tableType + " OPTIONS() AS (SELECT * FROM " + externalTableName + ")";
      else {
        String optionsString = StringUtils.join(options, ",");
        query = "CREATE TABLE " + tableName + " USING " + tableType + " OPTIONS(optionsString) AS (SELECT * FROM " + externalTableName + ")";
      }
      conn.createStatement().executeUpdate(query);
    }
  }

  public static String parseOptios() {
    Vector options = SnappyPrms.getTableOptions();
    String optionsString = StringUtils.join(options, ",");
    return optionsString;
  }

  public static void createAndLoadTablesForStabilityTest() throws SQLException {
    Connection conn = getLocatorConnection();
    String query;
    Vector tableNames = SnappyPrms.getTableList();
    Vector tableTypeList = SnappyPrms.getTableTypeList();
    if (tableTypeList.size() != tableNames.size()) {
      Log.getLogWriter().info("Adding last element in the tableTypeList for the  " +
          "tables for which no table type is specified.");
      while (tableTypeList.size() != tableNames.size())
        tableTypeList.add(tableTypeList.lastElement());
    }
    Vector insertTableNames = SnappyPrms.getInsertTableList();
    Vector parquetExternalTableNames = SnappyPrms.getParquetExternalTableList();
    Vector csvExternalTableNames = SnappyPrms.getCSVExternalTableList();
    Vector externalTableNamesForInsert = SnappyPrms.getExternalTableListForInsert();
    Vector dataPathListForParquet = SnappyPrms.getDataPathListForParquet();
    Vector dataPathListForCSV = SnappyPrms.getDataPathListForCSV();
    Vector externalTableNames = new HydraVector();
    externalTableNames.addAll(parquetExternalTableNames);
    externalTableNames.addAll(csvExternalTableNames);
    createAndLoadExternalTablesUsingParquet(parquetExternalTableNames, dataPathListForParquet, conn);
    createAndLoadExternalTablesUsingCSV(csvExternalTableNames, dataPathListForCSV, conn);
    createAndLoadTables(externalTableNames, tableNames, tableTypeList, conn);
    for (int i = 0; i < insertTableNames.size(); i++) {
      String insertTableName = (String) insertTableNames.elementAt(i);
      String externalTableName = (String) externalTableNamesForInsert.elementAt(i);
      query = "insert into " + insertTableName + " select * from " + externalTableName;
      conn.createStatement().executeUpdate(query);
    }
    closeConnection(conn);
  }

  public static void createViewsAndDependantTablesForStabilityTest() throws SQLException {
    Connection conn = getLocatorConnection();
    String query;
    query = "set snappydata.sql.hashAggregateSize=-1";
    conn.createStatement().executeUpdate(query);
    query = "set spark.driver.maxResultSize=6g";
    conn.createStatement().executeUpdate(query);
    query = "create or replace view revenue as select  l_suppkey as supplier_no, " +
        "sum(l_extendedprice * (1 - l_discount)) as total_revenue from LINEITEM where l_shipdate >= '1993-02-01'" +
        " and l_shipdate <  add_months('1996-01-01',3) group by l_suppkey";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR replace VIEW review_count_GT_10 AS SELECT * FROM (SELECT COUNT(*) AS " +
        "review_count,PRODUCT_ID FROM REVIEWS GROUP BY PRODUCT_ID) WHERE review_count > 10";
    conn.createStatement().executeUpdate(query);
    query = " CREATE OR REPLACE VIEW REVIEW_count_rating_GT_3 AS SELECT COUNT(*) AS review_count ," +
        "PRODUCT_ID FROM (SELECT PRODUCT_ID FROM REVIEWS WHERE STAR_RATING > 3) GROUP BY PRODUCT_ID";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR REPLACE VIEW REVIE_RANKINGS AS SELECT PRODUCT_ID,STAR_RATING,RANK() OVER" +
        " (PARTITION BY PRODUCT_ID ORDER BY REVIEW_DATE) AS REVIEW_NUMBER FROM REVIEWS";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR REPLACE VIEW COUNT_PER_RATING_REVIEW_NUMBER AS SELECT COUNT(*) COUNT," +
        " REVIEW_NUMBER, STAR_RATING FROM REVIE_RANKINGS GROUP BY REVIEW_NUMBER,STAR_RATING ORDER BY REVIEW_NUMBER";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR REPLACE VIEW COUNT_PER_REVIEW_NUMBER AS SELECT COUNT(*) COUNT," +
        "REVIEW_NUMBER FROM REVIE_RANKINGS GROUP BY REVIEW_NUMBER ORDER BY REVIEW_NUMBER";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR replace VIEW votes_by_category_and_marketplace AS SELECT CUSTOMER_ID,MARKETPLACE," +
        "product_category,TOTAL_VOTES,RANK() OVER (PARTITION BY MARKETPLACE,product_category ORDER BY " +
        "TOTAL_VOTES DESC) AS RANK FROM (SELECT CUSTOMER_ID,MARKETPLACE,product_category,SUM(TOTAL_VOTES) " +
        "AS TOTAL_VOTES FROM REVIEWS GROUP BY CUSTOMER_ID,MARKETPLACE,product_category)";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR REPLACE VIEW total_customers_per_marketplace_and_category AS SELECT " +
        "COUNT(customer_id) AS COUNT,marketplace,product_category FROM votes_by_category_and_marketplace " +
        "GROUP BY marketplace,product_category";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR REPLACE VIEW product_ratings AS SELECT MARKETPLACE,PRODUCT_ID,AVG(STAR_RATING)" +
        " AS avg_rating FROM REVIEWS GROUP BY MARKETPLACE,PRODUCT_ID";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR replace VIEW products_with_votes_lt_50 AS (SELECT * FROM (SELECT PRODUCT_ID, " +
        "COUNT(*) review_count FROM REVIEWS GROUP BY PRODUCT_ID) WHERE review_count < 50)";
    conn.createStatement().executeUpdate(query);
    query = "CREATE OR replace VIEW rating_gt_3_count AS SELECT PRODUCT_ID, COUNT(*) AS review_count " +
        "FROM (SELECT * FROM REVIEWS WHERE STAR_RATING > 3) GROUP BY PRODUCT_ID";
    conn.createStatement().executeUpdate(query);
    query = "create table IF NOT EXISTS weak_hearts using column as select patient from " +
        "(select patient, case when description = 'Major depression  single episode' then 1 else 0 end as " +
        "attack_once, case when description = 'Major depression disorder' then 1 else 0 end as attacked " +
        "from conditions) c group by patient having sum(attack_once) = 0 and sum(attacked) > 0";
    conn.createStatement().executeUpdate(query);
    query = "CREATE VIEW IF NOT EXISTS depression_patients_group_by_ethnicity  AS SELECT" +
        " p.ethnicity,COUNT(1)  AS COUNT FROM conditions c  JOIN patients p ON p.id = c.patient  " +
        "WHERE description LIKE '%Major depression disorder%' GROUP BY p.ethnicity";
    conn.createStatement().executeUpdate(query);
    query = "create view IF NOT EXISTS hview as select p.id, p.ETHNICITY from " +
        "patients p, conditions c  where p.id = c.patient and c.description = 'Hypertension'";
    conn.createStatement().executeUpdate(query);
    query = "create view IF NOT EXISTS hviewethgroup as select count(*) cnt, ETHNICITY  " +
        "from hview  group by ETHNICITY order by cnt desc";
    conn.createStatement().executeUpdate(query);
    query = "create view IF NOT EXISTS ethnicity_count_with_major_depression  as " +
        "select p.ETHNICITY, count(1) as count from PATIENTS as p, CONDITIONS as c " +
        "where p.ID = c.PATIENT and c.DESCRIPTION like 'Major depression disorder' group by 1";
    conn.createStatement().executeUpdate(query);
    query = "create view IF NOT EXISTS ethnicity_without_single_episode  " +
        "as select distinct(p2.ETHNICITY) from PATIENTS as p2, CONDITIONS as c2 where " +
        "p2.ID = c2.PATIENT and c2.DESCRIPTION not like 'Major depression  single episode'";
    conn.createStatement().executeUpdate(query);
    query = "create table IF NOT EXISTS q using column as select patient " +
        "from (select  *, case when description in " +
        "('Anti-suicide psychotherapy', 'Psychiatry care plan', " +
        "'Major depressive disorder clinical management plan') then 1 else 0 end " +
        " as coverage from careplans) c group by patient having sum(coverage) = 0";
    conn.createStatement().executeUpdate(query);
    query = "CREATE VIEW IF NOT EXISTS patient_expenses AS SELECT concat(p.first, ' ', p.LAST) AS " +
        "name, p.city,m.total_cost AS expenses  FROM patients p  JOIN (SELECT patient, " +
        "SUM(cost) AS total_cost  FROM ((SELECT patient, totalcost AS cost FROM medications) " +
        "UNION ALL  (SELECT patient, cost FROM immunizations ) UNION ALL  (SELECT patient, cost " +
        "FROM encounters)  UNION ALL (SELECT patient,cost FROM procedures))  GROUP BY patient ) m " +
        "ON p.id = m.patient";
    conn.createStatement().executeUpdate(query);
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
