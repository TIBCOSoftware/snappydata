package io.snappydata.hydra.hiveThriftServer;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsBB;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;

public class HiveThrifServerUtils extends SnappyTest {

    private Connection jdbcConnection = null;
    private Connection snappyJDBCConnection = null;
    public static HiveThrifServerUtils htsUtils = null;

    private Connection connectToBeeline() {
        htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.jdbcConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "app", "app");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in connecting to Beeline , " + se.getMessage());
        }
        return  htsUtils.jdbcConnection;
    }

    public static void HydraTask_CreateTableFromBeeline() {
        htsUtils = new HiveThrifServerUtils();
        Connection jdbcConnection = htsUtils.connectToBeeline();
        try {
            jdbcConnection.createStatement().execute("create table if not exists default.Student(id int, name String, subject String, marks int, tid int) using column");
            Log.getLogWriter().info("Table created successully");
            } catch(SQLException se) {
            Log.getLogWriter().info("Error in creating table from beeline , " + se.getMessage());
         }
         closeConnection(jdbcConnection);
    }

    public static void HydraTask_CreateFunction_subject() {
        htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
            htsUtils.snappyJDBCConnection.createStatement().execute("create function default.subject as io.snappydata.hydra.MySubject returns string using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/subject1.jar';");
            Statement st = htsUtils.snappyJDBCConnection.createStatement();
            ResultSet rs = st.executeQuery("select default.subject(2L)");
            while(rs.next()) {
                String s = rs.getString(1);
                Log.getLogWriter().info("Output from Function : " + s);
            }
            closeConnection(htsUtils.snappyJDBCConnection);
        } catch (SQLException se) {
            Log.getLogWriter().info("Error in creating function , " + se.getMessage());
        }
    }

    public static void HydraTask_DropFunction_subject() {
        htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
            htsUtils.snappyJDBCConnection.createStatement().execute("drop function default.subject;");
            closeConnection(htsUtils.snappyJDBCConnection);
            Log.getLogWriter().info("Function Dropped");
        } catch (SQLException se) {
            Log.getLogWriter().info("Error in dropping function , " + se.getMessage());
        }
    }

    public static void HydraTask_DropTableFromBeeline() {
        htsUtils = new HiveThrifServerUtils();
        Connection jdbcConnection = htsUtils.connectToBeeline();
        try {
            jdbcConnection.createStatement().execute("drop table if exists default.Student");
            Log.getLogWriter().info("Table Dropped");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from beeline , " + se.getMessage());
        }
        closeConnection(jdbcConnection);
    }

    public static void HydraTask_populateTableData() {
        htsUtils = new HiveThrifServerUtils();
        int tid = htsUtils.getMyTid();
        ArrayList<Integer> dmlthreads = null;
        if (SnappyDMLOpsBB.getBB().getSharedMap().containsKey("dmlThreads"))
            dmlthreads = (ArrayList<Integer>) SnappyDMLOpsBB.getBB().getSharedMap().get("dmlThreads");
        String tids = StringUtils.join(dmlthreads,",");
        dynamicAppProps.put(tid, "tids=\\\"" + tids + "\\\"");
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        htsUtils.executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
                jarPath, SnappyPrms.getUserAppName());

    }

    public static void HydraTask_performDMLOps() {
        htsUtils = new HiveThrifServerUtils();
        int tid = htsUtils.getMyTid();
        dynamicAppProps.put(tid, "tid=" + tid);
        String logFile = "snappyJobResult_thr_" + tid + "_" + System.currentTimeMillis() + ".log";
        htsUtils.executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, SnappyPrms.getUserAppJar(),
                jarPath, SnappyPrms.getUserAppName());

    }

}
