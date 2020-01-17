/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.hydra.preparedStmt;

import java.math.BigDecimal;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.sql.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JdbcTestPS implements Runnable {
  private String hostPort;
  private String threadName;
  private int thrdCnt;
  Map<String, Long> finalQueryMap;
  String path;
  protected String url = "jdbc:snappydata://";
  protected String driver = "io.snappydata.jdbc.ClientDriver";
  String[] queryArr = {"q2", "q8", "q9", "q10", "q13", "q16", "q23a", "q31", "q33", "q34", "q44", "q48", "q49", "q53", "q58", "q66", "q75", "q80"};
  String queryFilePath = "/export/dev9a/users/spillai/snappydata/dtests/src/resources/scripts/preparedStmt";

  public JdbcTestPS(String hostname, Integer threadN, Map<String, Long> finalMap, String filePath) {
    threadName = "Thread_" + threadN;
    hostPort = hostname;
    finalQueryMap = finalMap;
    path = filePath;
  }

  public void run() {
    executeSQLStmts(hostPort);
  }

  public Connection getConnection(String host_details) {
    Connection con = null;
    try {
      Class.forName(driver);
      con = DriverManager.getConnection(url + host_details);
    } catch (SQLException ex) {
      System.err.println("SQLException: " + ex.getMessage());
      System.exit(3);
    } catch (Exception ex) {
      System.out.println("Caught exception " + ex.getMessage());
    }
    return con;
  }

public void executeSQLStmts(String hostport) {
    Connection conn = null;
    try {
      conn = getConnection(hostport);
      for (int q = 0; q <= queryArr.length - 1; q++) {
        int count = 0;
        String queryName = queryArr[q];
        String filePath = queryFilePath + "/" + queryName + ".sql";
        String queryString = new String(Files.readAllBytes(Paths.get(filePath)));
        System.out.println("The query to be executed is " + queryName);
        PreparedStatement ps = conn.prepareStatement(queryString);
        switch (queryName) {
          case "q2":
            ps.setString(1, "Sunday");
            ps.setString(2, "Monday");
            ps.setString(3, "Tuesday");
            break;
          case "q8":
            ps.setString(1, "26231");
            ps.setString(2, "43848");
            ps.setString(3, "15126");
            ps.setString(4, "91137");
            ps.setString(5, "61265");
            ps.setString(6, "98294");
            ps.setString(7, "25782");
            break;
          case "q9":
            ps.setInt(1, 365541424);
            ps.setInt(2, 216357808);
            break;
          case "q10":
            ps.setString(1, "Rush County");
            ps.setString(2, "Toole County");
            ps.setString(3, "Jefferson County");
            ps.setString(4, "Dona Ana County");
            ps.setString(5, "La Porte County");
            ps.setInt(6, 2002);
            break;
          case "q13":
            ps.setString(1, "M");
            ps.setDouble(2, 100.00);
            ps.setDouble(3, 150.00);
            break;
          case "q16":
            for (int i = 1; i <= 2; i++)
              ps.setString(i, "2002-02-01");
            for (int i = 3; i <= 7; i++)
              ps.setString(i, "Williamson County");
            break;
          case "q23a":
            ps.setInt(1, 2000);
            ps.setInt(2, 2000 + 1);
            ps.setInt(3, 2000 + 2);
            ps.setInt(4, 2000 + 3);
            break;
          case "q28":
            ps.setInt(1, 459);
            ps.setInt(2, 31);
            ps.setInt(3, 79 + 20);
            ps.setInt(4, 26);
            ps.setInt(5, 7326 + 1000);
            break;
          case "q31":
            ps.setInt(1, 3);
            ps.setInt(2, 2000);
            ps.setInt(3, 0);
            break;
          case "q33":
            for (int i = 1; i <= 3; i++)
              ps.setString(i, "Electronics");
            break;
          case "q34":
            ps.setInt(1, 1);
            ps.setInt(2, 28);
            ps.setString(3, "unknown");
            break;
          case "q44":
            ps.setInt(1, 4);
            ps.setInt(2, 11);
            break;
          case "q48":
            ps.setString(1, "D");
            ps.setString(2, "2 yr Degree");
            ps.setDouble(3, 50.00);
            ps.setDouble(4, 100.00);
            break;
          case "q49":
            System.out.println("Setting int value for "  );
            ps.setInt(1, 10000);
            ps.setInt(2, 2001);
            ps.setInt(3,12);
            break;
          case "q53":
            ps.setString(1, "scholaramalgamalg #14");
            ps.setString(2, "scholaramalgamalg #7");
            ps.setString(3, "exportiunivamalg #9");
            ps.setString(4, "scholaramalgamalg #9");
            break;
          case "q58":
              for (int i = 1; i <= 3; i++) {
              System.out.println("Setting string value for " +i );
              ps.setString(i, "2000-01-03");
            }
            for (int i = 4; i <= 9; i++) {
              ps.setBigDecimal(i, new BigDecimal(0.9));
 System.out.println("Finished Setting double value for " +i );
            }
            break;
          case "q66":
            ps.setInt(1,12);
            ps.setInt(2,30838);
            ps.setInt(3,5);
            ps.setString(4,"DHL");
            ps.setString(5,"BARIAN");
            break;
          case "q75":
            for(int i=1;i<=3;i++)
              ps.setString(i,"Books");
            break;
          case "q77":
            for(int i=1;i<=12;i++)
              ps.setString(i,"2000-08-03");
            break;
          case "q80":
            for (int i = 1; i <= 6; i++)
              ps.setString(i, "2000-08-03");
            break;
        }

        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
          ++count;
        }
        System.out.println("Total row count = " + count + "\n");
      }
    } catch (IOException ex) {
      System.out.println("Caught exception " + ex.getMessage());
    } catch (SQLException se) {
      System.out.println("QUERY FAILED. Exception is : \n" + se
          .getSQLState() + " : " + se.getMessage());
      while (se != null) {
        System.out.println(se.getCause());
        se = se.getNextException();
      }
    } finally {
      try {
        conn.close();
      } catch (SQLException se) {
        System.out.println("Failed to close the connection " + se.getMessage());
      }
    }
  }

  public static void main(String args[]) {
    String hostName = args[0];
    int threadCnt = Integer.parseInt(args[1]);
    String filePath = args[2];
    ExecutorService executor = Executors.newFixedThreadPool(threadCnt);
    Map<String, Long> perThrdAvg = new HashMap<>();
    for (int i = 1; i <= threadCnt; i++) {
      String threadName = "Thread-" + i;
      System.out.println("Creating " + threadName);
      executor.execute(new JdbcTestPS(hostName, i, perThrdAvg, filePath));
    }
    executor.shutdown();
    try {
      executor.awaitTermination(3600, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      System.out.println("Got Exception while waiting for all threads to complete populate" +
          " tasks");
    }
    long finalSum = 0l;
    for (Map.Entry<String, Long> val : perThrdAvg.entrySet()) {
      finalSum += val.getValue();
    }
 }
}
