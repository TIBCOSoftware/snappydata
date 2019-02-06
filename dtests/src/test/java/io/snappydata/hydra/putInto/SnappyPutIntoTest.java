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
package io.snappydata.hydra.putInto;

import java.sql.SQLException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Vector;

import hydra.Log;
import hydra.TestConfig;
import io.snappydata.hydra.cdcConnector.SnappyCDCPrms;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;


public class SnappyPutIntoTest extends SnappyTest {

  public static int numThreads = TestConfig.tasktab().intAt(SnappyPrms.numThreadsForConcExecution, TestConfig.tab().
      intAt(SnappyPrms.numThreadsForConcExecution, 15));

  public static void HydraTask_concPutIntoUsingJDBCConn() {

    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.concPutInto(primaryLocatorHost, primaryLocatorPort, numThreads);
  }

  public static void HydraTask_concSelectUsingJDBCConn() {
    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.conSelect(primaryLocatorHost, primaryLocatorPort, numThreads);
  }

  public static void HydraTask_bulkDelete(){
    try {
      Log.getLogWriter().info("Inside bulkDelete");
      Connection conn = SnappyTest.getLocatorConnection();
      Vector tableNames = SnappyCDCPrms.getNodeName();
      int deleteID = 0;
      int minID = 0;
      for(int i = 0 ;i < tableNames.size();i++) {
        String minQ = "SELECT min(ID) FROM " + tableNames.elementAt(i);
        String selectQ = "SELECT count(*) FROM " + tableNames.elementAt(i);
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery(selectQ);
        while (rs.next())
          count = rs.getInt(1);
        if (count > 10000) {
          ResultSet rs1 = conn.createStatement().executeQuery(minQ);
          while (rs1.next())
            minID = rs1.getInt(1);
          deleteID = minID + 1000;
          Log.getLogWriter().info("The min id is  " + minID + " the delete id is " + deleteID);
          conn.createStatement().execute("DELETE FROM " + tableNames.elementAt(i) + " WHERE ID < " + deleteID);
        }
      }
    }
    catch (SQLException ex) {
      throw new util.TestException("Caught exception in HydraTask_bulkDelete() " + ex.getMessage());
    }
  }

}
