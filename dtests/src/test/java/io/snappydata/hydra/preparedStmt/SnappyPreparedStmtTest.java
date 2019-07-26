package io.snappydata.hydra.preparedStmt;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
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
  String[] queryArr = {"q2", "q8", "q9", "q10", "q13", "q16", "q23a", "q31", "q33", "q34", "q44", "q48", "q49", "q53", "q58", "q66", "q75", "q80",
      "q4", "q6", "q11", "q15", "q18", "q19", "q26", "q27", "q38", "q41", "q46", "q47", "q50", "q56", "q57",
      "q21", "q97", "q96", "q95", "q94", "q92", "q89", "q87", "q86", "q85", "q84", "q83", "q69", "q65", "q64", "q63", "q59"};
  String[] dateCol = {"'2000-03-11'", "'2000-01-27'", "'1999-02-01'"};
  Integer[] dYearVal = {1999, 2000, 2001};
  Integer[] intVal = {1200, 1212, 1201};
  Double[] doubleVal = {50.00, 10.00, 100.00};
  String[] categoryVal = {"'Books'", "'Electronics'", "'Jewelry'"};
  String[] ca_cityVal = {"'Springfield'", "'Maple Grove'", "'Edgewood'"};


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
    Vector queryFile = SnappyPrms.getDataLocationList();
    String queryFilePath = queryFile.get(0).toString();
    String queryFilePathPS = queryFile.get(1).toString();
    try {
      conn = getLocatorConnection();
      for (int q = 0; q <= queryArr.length - 1; q++) {
        String queryName = queryArr[q];
        boolean isChangingConstant = false;
        String filePath = queryFilePathPS + "/" + queryName + ".sql";
        String queryStringPS = new String(Files.readAllBytes(Paths.get(filePath)));
        String tempQueryStringPS = queryStringPS;
        String queryStringWithCC = null;
        Log.getLogWriter().info("The query to be executed is " + queryStringPS);
        PreparedStatement ps = conn.prepareStatement(queryStringPS);
        for (int j = 0; j < 3; j++) {
          switch (queryName) {
            case "q21":
              for (int i = 1; i <= 4; i++) {
                String subStr = dateCol[j].substring(1, dateCol[j].length() - 1);
                Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
                ps.setString(i, subStr);
              }
              queryStringWithCC = tempQueryStringPS.replace("?", dateCol[j]);
              isChangingConstant = true;
              break;
            case "q97":
              for (int i = 1; i <= 4; i++)
                ps.setInt(i, intVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", intVal[j].toString());
              isChangingConstant = true;
              break;
            case "q96":
              ps.setString(1, "ese");
              break;
            case "q95":
              for (int i = 1; i <= 2; i++) {
                String subStr = dateCol[j].substring(1, dateCol[j].length() - 1);
                Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
                ps.setString(i, subStr);
              }
              queryStringWithCC = tempQueryStringPS.replace("?", dateCol[j]);
              isChangingConstant = true;
              break;
            case "q94":
              ps.setString(1, "IL");
              break;
            case "q92":
              for (int i = 1; i <= 4; i++) {
                String subStr = dateCol[j].substring(1, dateCol[j].length() - 1);
                Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
                ps.setString(i, subStr);
              }
              queryStringWithCC = tempQueryStringPS.replace("?", dateCol[j]);
              isChangingConstant = true;
              break;
            case "q89":
              ps.setInt(1, dYearVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", dateCol[j]);
              isChangingConstant = true;

              break;
            case "q87":
              for (int i = 1; i <= 6; i++)
                ps.setInt(i, intVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", intVal[j].toString());
              isChangingConstant = true;
              break;
            case "q86":
              for (int i = 1; i <= 2; i++)
                ps.setInt(i, 0);
              break;
            case "q85":
              for (int i = 1; i <= 2; i++)
                ps.setDouble(i, doubleVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", doubleVal[j].toString());
              isChangingConstant = true;
              break;
            case "q84": {
              String subStr = ca_cityVal[j].substring(1, ca_cityVal[j].length() - 1);
              Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
              ps.setString(1, subStr);
            }
            queryStringWithCC = tempQueryStringPS.replace("?", ca_cityVal[j]);
            isChangingConstant = true;
            break;
            case "q83":
              for (int i = 1; i <= 3; i++) {
                String subStr = dateCol[j].substring(1, dateCol[j].length() - 1);
                Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
                ps.setString(i, subStr);
              }
              queryStringWithCC = tempQueryStringPS.replace("?", dateCol[j]);
              isChangingConstant = true;
              break;
            case "q77":
              for (int i = 1; i <= 12; i++)
                ps.setString(i, "2000-08-03");
              break;
            case "q69":
              for (int i = 1; i <= 6; i++)
                ps.setInt(i, 4);
              break;
            case "q65":
              for (int i = 1; i <= 4; i++)
                ps.setInt(i, intVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", intVal[j].toString());
              isChangingConstant = true;
              break;
            case "q64":
              ps.setInt(1, 2);
              break;
            case "q63":
              for (int i = 1; i <= 12; i++)
                ps.setInt(i, intVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", intVal[j].toString());
              isChangingConstant = true;
              break;
            case "q59":
              for (int i = 1; i <= 4; i++)
                ps.setInt(i, intVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", intVal[j].toString());
              isChangingConstant = true;
              break;
            case "q2":
              ps.setString(1, "Sunday");
              ps.setString(2, "Monday");
              ps.setString(3, "Tuesday");
              break;
            case "q4":
              ps.setString(1, "s");
              ps.setString(2, "c");
              ps.setString(3, "w");
              break;
            case "q6":
              ps.setDouble(1, 1.2);
              ps.setInt(2, 10);
              break;
            case "q11":
              for (int i = 1; i <= 4; i++)
                ps.setInt(i, 0);
              break;
            case "q15":
              ps.setString(1, "80348");
              ps.setString(2, "GA");
              break;
            case "q18":
              ps.setString(1, "F");
              ps.setString(2, "IN");
              break;
            case "q19":
              ps.setInt(1, 1);
              ps.setInt(2, 5);
              break;
            case "q26":
              for (int i = 1; i <= 2; i++)
                ps.setString(i, "N");
              break;
            case "q27":
              for (int i = 1; i <= 6; i++)
                ps.setString(i, "TN");
              break;
            case "q38":
              for (int i = 1; i <= 6; i++)
                ps.setInt(i, intVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", intVal[j].toString());
              isChangingConstant = true;
              break;
            case "q41":
              ps.setString(1, "Men");
              ps.setString(2, "Women");
              break;
            case "q46":
              ps.setInt(1, 4);
              ps.setInt(2, 0);
              for (int i = 3; i <= 6; i++)
                ps.setString(i, "Fairview");
              break;
            case "q47":
              for (int i = 1; i <= 5; i++)
                ps.setInt(i, 1);
              ps.setDouble(6, 0.1);
              break;
            case "q50":
              for (int i = 1; i <= 2; i++)
                ps.setInt(i, 120);
              break;
            case "q56":
              for (int i = 1; i <= 3; i++)
                ps.setInt(i, -5);
              break;
            case "q57":
              for (int i = 1; i <= 4; i++)
                ps.setInt(1, dYearVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", dateCol[j]);
              isChangingConstant = true;
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
              ps.setDouble(2, doubleVal[j]);
              ps.setDouble(3, 150.00);
              queryStringWithCC = tempQueryStringPS.replace("?", doubleVal[j].toString());
              isChangingConstant = true;
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
              for (int i = 1; i <= 3; i++) {
                String subStr = categoryVal[j].substring(1, categoryVal[j].length() - 1);
                Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
                ps.setString(i, subStr);
              }
              queryStringWithCC = tempQueryStringPS.replace("?", categoryVal[j]);
              isChangingConstant = true;
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
              ps.setDouble(4, doubleVal[j]);
              queryStringWithCC = tempQueryStringPS.replace("?", doubleVal[j].toString());
              isChangingConstant = true;
              break;
            case "q49":
              ps.setInt(1, 10000);
              for (int i = 2; i <= 5; i++)
                ps.setDouble(i, 10);
              break;
            case "q53":
              ps.setString(1, "scholaramalgamalg #14");
              ps.setString(2, "scholaramalgamalg #7");
              ps.setString(3, "exportiunivamalg #9");
              ps.setString(4, "scholaramalgamalg #9");
              break;
            case "q58":
              for (int i = 1; i <= 3; i++) {
                ps.setString(i, "2000-01-03");
              }
              for (int i = 4; i <= 9; i++) {
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
              for (int i = 1; i <= 3; i++) {
                String subStr = categoryVal[j].substring(1, categoryVal[j].length() - 1);
                Log.getLogWriter().info("The subString for " + queryName + " = " + subStr);
                ps.setString(i, subStr);
              }
              queryStringWithCC = tempQueryStringPS.replace("?", categoryVal[j]);
              isChangingConstant = true;
              break;
            case "q80":
              for (int i = 1; i <= 6; i++)
                ps.setString(i, "2000-08-23");
              break;
          }
          Log.getLogWriter().info("Executing query : " + queryName);
          ResultSet rsPS = ps.executeQuery();
          Log.getLogWriter().info("Executed query : " + queryName);
          Log.getLogWriter().info("Executing non ps query for " + queryFilePath + "/" + queryName + ".sql");
          String queryString = null;

          if (!isChangingConstant) {
            queryString = new String(Files.readAllBytes(Paths.get(queryFilePath + "/" + queryName + ".sql")));
          } else {
            queryString = queryStringWithCC;
            Log.getLogWriter().info("The query with changing constant is " + queryString);
          }

          ResultSet rs = conn.createStatement().executeQuery(queryString);

          validateResultSet(rs, rsPS, queryName);
        }
      }
    } catch (IOException ex) {
      throw new TestException("Caught exception " + ex.getMessage());

    } catch (SQLException se) {
      throw new TestException("QUERY FAILED. Exception is : \n" + se.getSQLState() + " : " + se.getMessage());
    } finally {
      try {
        conn.close();
      } catch (SQLException se) {

        throw new TestException("Failed to close the connection " + se.getMessage());

      }
    }
  }

  public void validateResultSet(ResultSet rs, ResultSet rsPS, String queryName) {
    try {
      SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();
      String logFile = getCurrentDirPath();
      String outputFile,outputFilePS;
      StructTypeImpl snappyStrtTyp = ResultSetHelper.getStructType(rs);
      List<Struct> snappyList = ResultSetHelper.asList(rs, snappyStrtTyp, false);
      outputFile = logFile + File.separator + queryName + ".out";
      Log.getLogWriter().info("The total num of rows for  " + queryName + " without prepared statement is  = " + snappyList.size());
      testInstance.listToFile(snappyList, outputFile);
      rs.close();

      StructTypeImpl snappyPSStrtTyp = ResultSetHelper.getStructType(rsPS);
      List<Struct> snappyPSList = ResultSetHelper.asList(rsPS, snappyPSStrtTyp, false);
      outputFilePS = logFile + File.separator + queryName + "_PS.out";
      Log.getLogWriter().info("The total num of rows for  " + queryName + " with prepared statement is  = " + snappyPSList.size());
      testInstance.listToFile(snappyPSList, outputFilePS);
      rsPS.close();

      Log.getLogWriter().info("Heading for full resultSet validation");
      testInstance.compareFiles(logFile, outputFile, outputFilePS, true, queryName + System.currentTimeMillis());

    } catch (SQLException ex) {
      throw new TestException("Caught SQLException " + ex.getMessage());
    }

  }
}
