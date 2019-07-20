package io.snappydata.hydra.preparedStmt;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import io.snappydata.test.util.TestException;
import sql.sqlutil.ResultSetHelper;


public class SnappyPreparedStmtTest extends SnappyTest {
  public static SnappyPreparedStmtTest snappyPreparedStmtTest;
  String[] queryArr = {"q2"};//, "q8", "q9", "q10", "q13", "q16", "q23a", "q28", "q31", "q33", "q34", "q44", "q48", "q49", "q53","q66", "q75", "q80"};
  //q58

  public SnappyPreparedStmtTest() {
  }

  public static void HydraTask_executeOps() {
    if (snappyPreparedStmtTest == null) {
      snappyPreparedStmtTest = new SnappyPreparedStmtTest();
    }
    snappyPreparedStmtTest.executePreparedStmts();
  }

  public void executePreparedStmts() {
    Connection conn = null;
    Connection conn1 = null;
    Vector queryFile = SnappyPrms.getDataLocationList();
    String queryFilePath = queryFile.get(0).toString();
    String queryFilePathPS = queryFile.get(1).toString();
    try {
      conn = getLocatorConnection();
      conn1 = getLocatorConnection();
      for (int q = 0; q <= queryArr.length - 1; q++) {

        String queryName = queryArr[q];
        String filePath = queryFilePathPS + "/" + queryName + ".sql";
        String queryStringPS = new String(Files.readAllBytes(Paths.get(filePath)));
        Log.getLogWriter().info("The query to be executed is " + queryStringPS);
        PreparedStatement ps = conn.prepareStatement(queryStringPS);
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
            ps.setDouble(2, 150.00);
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
            ps.setDouble(1, 10000);
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
              Log.getLogWriter().info("Setting string value for " +i );
              ps.setString(i, "2000-01-03");
            }
            for (int i = 4; i <= 9; i++) {
              Log.getLogWriter().info("Setting double value for " +i );
              ps.setDouble(i, Double.parseDouble(0.9 + ""));
            }
            break;
          case "q66":
            ps.setInt(1, 12);
            ps.setInt(2, 30838);
            ps.setInt(3, 5);
            ps.setString(4, "DHL");
            ps.setString(5, "BARIAN");
            break;
          case "q75":
            for (int i = 1; i <= 3; i++)
              ps.setString(i, "Books");
            break;
          case "q80":
            for (int i = 1; i <= 6; i++)
              ps.setString(i, "2000-08-03");
            break;
         }

        ResultSet rsPS = ps.executeQuery();
        String queryString = new String(Files.readAllBytes(Paths.get(queryFilePath + "/" + queryName + ".sql")));
        ResultSet rs = conn1.createStatement().executeQuery(queryString);

        validateResultSet(rs, rsPS, queryName);
      }
    } catch (IOException ex) {
      Log.getLogWriter().info("Caught exception " + ex.getMessage());
    } catch (SQLException se) {
      Log.getLogWriter().info("QUERY FAILED. Exception is : \n" + se
          .getSQLState() + " : " + se.getMessage());
      while (se != null) {
        Log.getLogWriter().info(se.getCause());
        se = se.getNextException();
      }
    } finally {
      try {
        conn.close();
        conn1.close();
      } catch (SQLException se) {
        Log.getLogWriter().info("Failed to close the connection " + se.getMessage());
      }
    }
  }

  public void validateResultSet(ResultSet rs, ResultSet rsPS, String queryName) {
    try {
      SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
      int count = 0;
      int countPS = 0;
      String logFile = getCurrentDirPath();
      String outputFile = null;
      String outputFilePS = null;

      StructTypeImpl snappyStrtTyp = ResultSetHelper.getStructType(rs);
      List<Struct> snappyList = ResultSetHelper.asList(rs, snappyStrtTyp, false);
      outputFile = logFile + File.separator + queryName + ".out";
      testInstance.listToFile(snappyList,outputFile);
      while (rs.next()) {
        ++count;
      }
      rs.close();

      StructTypeImpl snappyPSStrtTyp = ResultSetHelper.getStructType(rsPS);
      List<Struct> snappyPSList = ResultSetHelper.asList(rsPS, snappyPSStrtTyp, false);
      outputFilePS = logFile + File.separator + queryName + "_PS.out";
      testInstance.listToFile(snappyPSList,outputFilePS);
      while (rsPS.next()) {
        ++countPS;
      }
      rsPS.close();

      Log.getLogWriter().info("Total row count for preparedStatment query " + queryName + " is = " + count + "\n");
      if (count != countPS) {
        throw new TestException("For query " + queryName + " the query count with prepared statement " + count + " is not equal" +
            " to that without prepared statement " + countPS);
      } else {
        Log.getLogWriter().info("The count for normal query " + queryName + " = " + count +
            "\n The count for preparedStmt query "+ queryName + " = " + countPS );
        Log.getLogWriter().info("Heading for full resultSet validation");
        testInstance.compareFiles(logFile,outputFile,outputFilePS,false,queryName);
      }

    } catch (SQLException ex) {
      throw new TestException("Caught SQLException " + ex.getMessage());
    }

  }
}
