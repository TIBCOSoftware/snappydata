/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
import sql.sqlutil.ResultSetHelper;
import util.TestException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.Vector;

public class SnappyConcurrencyTest extends SnappyTest {

  public static void runPointLookUpQueries() throws SQLException {
    Vector<String> queryVect = SnappyPrms.getPointLookUpQueryList();
    String query = null;
    int queryNum = new Random().nextInt(queryVect.size());
    query = (String) queryVect.elementAt(queryNum);
    Connection conn = getLocatorConnection();
    ResultSet rs;
    try {
      rs = conn.createStatement().executeQuery(query);
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
      SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numpointLookUpQueriesExecuted);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing select query.", se);
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
    int queryNum = new Random().nextInt(queryVect.size());
    query = (String) queryVect.elementAt(queryNum);
    ResultSet rs = conn.createStatement().executeQuery(query);
    SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numQueriesExecuted);
    SnappyBB.getBB().getSharedCounters().increment(SnappyBB.numAggregationQueriesExecuted);
    /*StructTypeImpl sti = ResultSetHelper.getStructType(rs);
    List<Struct> queryResult = ResultSetHelper.asList(rs, sti, false);
    Log.getLogWriter().info("SS - Result for query : " + query + "\n" + queryResult.toString());*/
    closeConnection(conn);
  }

  public static void validateNumQueriesExecuted() throws SQLException {
    int numQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numQueriesExecuted);
    int numpointLookUpQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numpointLookUpQueriesExecuted);
    int numAggregationQueriesExecuted = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numAggregationQueriesExecuted);
    Log.getLogWriter().info("Total number of queries executed : " + numQueriesExecuted);
    Log.getLogWriter().info("Total number of pointLookUp queries executed : " +
        numpointLookUpQueriesExecuted);
    Log.getLogWriter().info("Total number of analytical queries executed : " + numAggregationQueriesExecuted);
  }

}
