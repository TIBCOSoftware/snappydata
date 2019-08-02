package io.snappydata.hydra.deployUnDeploy;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import hydra.Log;
import hydra.TestConfig;
import io.snappydata.hydra.cdcConnector.SnappyCDCPrms;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;

public class SnappyDeployUnDeployTest extends SnappyTest {

  public static SnappyDeployUnDeployTest snappyDeployUnDeployTest;
  public String jarAlias = "cassandraJar";

  public SnappyDeployUnDeployTest() {
  }

  public static void HydraTask_closeCassandraCluster() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.closeCassandraCluster();
  }

  public void closeCassandraCluster() {
    try {
      String cmd = "pkill -f CassandraDaemon";
      String dest = getCurrentDirPath() + File.separator + "cassandraClusterStop.log";
      File logFile = new File(dest);
      ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
      snappyTest.executeProcess(pb, logFile);
    } catch (Exception ex) {
      throw new TestException("Exception while closing Cassandra Cluster " + ex.getMessage());
    }
  }

  public static void HydraTask_startCassandraCluster() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.startCassandraCluster();
  }

  public void startCassandraCluster() {
    String cassPath = SnappyCDCPrms.getSnappyFileLoc();
    String scripPath = SnappyPrms.getDataLocationList().get(0).toString();
    String dest = getCurrentDirPath() + File.separator + "cassandraClusterStart.log";
    File logFile = new File(dest);
    try {
      ProcessBuilder pbClustStart = new ProcessBuilder(cassPath + "/bin/cassandra");
      Long startTime1 = System.currentTimeMillis();
      snappyTest.executeProcess(pbClustStart, logFile);
      Long totalTime1 = (System.currentTimeMillis() - startTime1);
      Log.getLogWriter().info("The cassandra cluster took " + totalTime1 + " ms to start up");

      ProcessBuilder pbScript = new ProcessBuilder(cassPath + "/bin/cqlsh", "-f", scripPath);
      snappyTest.executeProcess(pbScript, logFile);
      Log.getLogWriter().info("The cassandra script execution completed");
    } catch (Exception ex) {
      throw new TestException("Exception while start Cassandra Cluster" + ex.getMessage());
    }
  }

  public static void HydraTask_deployPkg() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.deployPkg();
  }

  public void deployPkg() {

    String pkgName = "com.datastax.spark:spark-cassandra-connector_2.11:2.0.7";
    String pkgPath = SnappyPrms.getDataLocationList().get(0).toString();
    try {
      Connection conn = getLocatorConnection();
      String deployStr = "deploy package " + jarAlias + " '" + pkgName + "' path '" + pkgPath + "' ";
      Log.getLogWriter().info("The deployStr is " + deployStr);
      conn.createStatement().execute(deployStr);
    } catch (Exception ex) {
      throw new TestException("Exception while deploying jar " + ex.getMessage());
    }
  }

  public void deployJar() {
    String jarName = "com.datastax.spark_spark-cassandra-connector_2.11-2.0.7.jar";
    String jarPath = SnappyPrms.getDataLocationList().get(0).toString() + "/" + jarName;
    try {
      //deploy package kafkaSource 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1' path '/home/smahajan/Downloads';
      Connection conn = getLocatorConnection();
      String deployStr = "deploy jar " + jarAlias + " '" + jarPath + "'";
      Log.getLogWriter().info("The deployStr is " + deployStr);
      conn.createStatement().execute(deployStr);
    } catch (Exception ex) {
      throw new TestException("Exception while deploying jar " + ex.getMessage());
    }
  }

  public static void HydraTask_unDeployPkg() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.unDeployPkg();
  }

  public void unDeployPkg() {
    try {
      //undeploy packageAlias;
      Connection conn = getLocatorConnection();
      String unDeployStr = "undeploy " + jarAlias;
      Log.getLogWriter().info("The deployStr is " + unDeployStr);
      conn.createStatement().execute(unDeployStr);
    } catch (Exception ex) {
      throw new TestException("Exception while undeploying jar " + ex.getMessage());
    }
  }

  public static void HydraTask_createFunction() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.createFunction();
  }

  public void createFunction() {
    String pkg = "io.snappydata.hydra.deployUnDeploy.udfFiles";
    Vector udfs = SnappyDeployUnDeployPrms.getUdfName();
    Vector returnTyp = SnappyDeployUnDeployPrms.getReturnType();
    String jarPath = TestConfig.tab().stringAt(SnappyPrms.snappyPocJarPath, null);
    //"/home/supriya/snappy/snappydata/dtests/build-artifacts/scala-2.11/libs/snappydata-store-scala-tests-0.1.0-SNAPSHOT-tests.jar";

    try {
      Connection conn = getLocatorConnection();
      for (int i = 0; i <= udfs.size() - 1; i++) {
        String udfAlias = udfs.get(i).toString().toLowerCase();
        String createFunc = "CREATE FUNCTION " + udfAlias + " as " + pkg + "." + udfs.get(i) +
            " returns " + returnTyp.get(i) + " using jar " + "'" + jarPath + "'";
        Log.getLogWriter().info("The function to be created is " + createFunc);
        conn.createStatement().execute(createFunc);
      }
    } catch (Exception ex) {
      throw new TestException("Exception while creating function with udfs " + ex.getMessage());
    }
  }

  public static void HydraTask_executeUDFFunction() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.executeUDFFunction();
  }

  public void executeUDFFunction() {
    Vector udfs = SnappyDeployUnDeployPrms.getUdfName();
    Boolean isExpExpected = SnappyDeployUnDeployPrms.getIsExpectedExecption();
    sleepForMs(10);
    try {
      Connection conn = getLocatorConnection();
      int argNum = 25;
      String argStr = "snappydata";
      String selectStr = "";

      for (int i = 0; i <= udfs.size() - 1; i++) {
        String udfName = udfs.get(i).toString();
        String udfAlias = udfs.get(i).toString().toLowerCase();
        Log.getLogWriter().info("SP:UDF name = " + udfName);
        switch (udfName) {
          case "MyUDF3":
            selectStr = "SELECT " + udfAlias + "(" + argNum + "," + argNum + ")";
            Log.getLogWriter().info("The select string is " + selectStr);
            break;
          case "MyUDF4":
            selectStr = "SELECT " + udfAlias + "(" + argNum + ")";
            Log.getLogWriter().info("The select string is " + selectStr);
            break;
          case "MyUDF5":
            selectStr = "SELECT " + udfAlias + "('" + argStr + "')";
            Log.getLogWriter().info("The select string is " + selectStr);
            break;
        }
        ResultSet rs = conn.createStatement().executeQuery(selectStr);
        validateResults(rs, udfName, argNum, argStr, isExpExpected);
        rs.close();
        // conn.close();
      }
    } catch (Exception ex) {
      if (isExpExpected)
        Log.getLogWriter().info("The exception is expected " + ex.getMessage());
      else
        throw new TestException("Exception while executing function with udfs " + ex.getMessage());
    }
  }

  public void validateResults(ResultSet rs, String udfName, Integer argNum, String argStr, Boolean isExceptionExpected) {
    try {
      switch (udfName) {
        case "MyUDF3":
          int expectedResult = argNum + argNum;
          int actualResult = 0;
          while (rs.next())
            actualResult = rs.getInt(1);
          Log.getLogWriter().info("The expectedResult is = " + expectedResult);
          Log.getLogWriter().info("The actualResult is = " + actualResult);
          if (expectedResult != actualResult)
            throw new TestException("Exception while validation");
          else
            Log.getLogWriter().info("The results match");
          break;
        case "MyUDF4":
          Float expectedResultFL = argNum / 100.0f;
          Float actualResultFL = 0f;
          while (rs.next())
            actualResultFL = rs.getFloat(1);
          Log.getLogWriter().info("The expectedResult is = " + expectedResultFL);
          Log.getLogWriter().info("The actualResult is = " + actualResultFL);
          if (Float.compare(expectedResultFL, actualResultFL) != 0)
            throw new TestException("Exception while validation");
          else
            Log.getLogWriter().info("The results match");
          break;
        case "MyUDF5":
          String expectedResultStr = argStr.toUpperCase();
          String actualResultStr = "";
          while (rs.next())
            actualResultStr = rs.getString(1);
          Log.getLogWriter().info("The expectedResult is = " + expectedResultStr);
          Log.getLogWriter().info("The actualResult is = " + actualResultStr);
          if (!expectedResultStr.equals(actualResultStr))
            throw new TestException("Exception while validation");
          else
            Log.getLogWriter().info("The results match");
          break;
      }
    } catch (Exception ex) {
      throw new TestException("Exception while validation" + ex.getMessage());
    }
  }

  public static void HydraTask_dropFunction() {
    if (snappyDeployUnDeployTest == null) {
      snappyDeployUnDeployTest = new SnappyDeployUnDeployTest();
    }
    snappyDeployUnDeployTest.dropFunction();
  }

  public void dropFunction() {
    try {
      Connection conn = getLocatorConnection();
      Vector udfs = SnappyDeployUnDeployPrms.getUdfName();
      for (int i = 0; i <= udfs.size() - 1; i++) {
        String udfAlias = udfs.get(i).toString().toLowerCase();
        String dropFunc = "DROP FUNCTION IF EXISTS " + udfAlias;
        Log.getLogWriter().info("The function to be dropped is " + dropFunc);
        conn.createStatement().execute(dropFunc);
      }
    } catch (Exception ex) {
      throw new TestException("Exception while dropping function" + ex.getMessage());
    }
  }

  public void listUDFS(Connection conn) {
    try {
      String cmd = " list jars";
      ResultSet rs = conn.createStatement().executeQuery(cmd);

    } catch (Exception ex) {

    }
  }


}
