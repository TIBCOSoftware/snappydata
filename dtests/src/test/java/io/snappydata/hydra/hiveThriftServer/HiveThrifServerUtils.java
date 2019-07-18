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
        htsUtils.jdbcConnection = htsUtils.connectToBeeline();
        try {
            htsUtils.jdbcConnection.createStatement().execute("create schema beelineDB");
            //Below line needs to be added when external hive meta store merged into the master.
            //htsUtils.jdbcConnection.createStatement().execute("create table if not exists beelineDB.Student(id int, name String, subject String, marks int, tid int) row format delimited fields terminated by ','");
            //htsUtils.jdbcConnection.createStatement().execute("load data local inpath '/home/cbhatt/hts1/' overwrite into table beelineDB.Student");
            htsUtils.jdbcConnection.createStatement().execute("create external table if not exists beelineDB.Student using csv options(path '/home/cbhatt/hts1/data.csv/')");
            Log.getLogWriter().info("Table beelineDB.Student created successfully in beeline");
            Statement st = htsUtils.jdbcConnection.createStatement();
            ResultSet rs = st.executeQuery("select * from beelineDB.Student limit 10");
            while (rs.next()) {
                Log.getLogWriter().info(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5) );
            }
            } catch(SQLException se) {
            Log.getLogWriter().info("Error in creating table from beeline , " + se.getMessage());
         }
         closeConnection(htsUtils.jdbcConnection);
    }

    public static void HydraTask_CreateTableFromSnappy() {
        htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
            htsUtils.snappyJDBCConnection.createStatement().execute("create schema snappyDB");
//            htsUtils.snappyJDBCConnection.createStatement().execute("create table if not exists snappyDB.Student(id int, name String, subject String, marks int, tid int) using column");
            htsUtils.snappyJDBCConnection.createStatement().execute("create external table if not exists snappyDB.Student using csv options(path '/home/cbhatt/hts1/data.csv/')");
            Log.getLogWriter().info("Table snappyDB.Student created successfully in snappy");
            Statement st = htsUtils.snappyJDBCConnection.createStatement();
            ResultSet rs = st.executeQuery("select * from snappyDB.Student limit 10");
            while (rs.next()) {
                Log.getLogWriter().info(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5) );
            }

            Log.getLogWriter().info("Table snappyDB.Student created successfully in Snappy");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in creating table from snappy , " + se.getMessage());
        }
        closeConnection(htsUtils.snappyJDBCConnection);
    }

//    public static void HydraTask_CreateFunction_subject() {
//        htsUtils = new HiveThrifServerUtils();
//        try {
//            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
//            htsUtils.snappyJDBCConnection.createStatement().execute("create function default.subject as io.snappydata.hydra.MySubject returns string using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/subject1.jar';");
//            Statement st = htsUtils.snappyJDBCConnection.createStatement();
//            ResultSet rs = st.executeQuery("select default.subject(2L)");
//            while(rs.next()) {
//                String s = rs.getString(1);
//                Log.getLogWriter().info("Output from Function : " + s);
//            }
//            closeConnection(htsUtils.snappyJDBCConnection);
//        } catch (SQLException se) {
//            Log.getLogWriter().info("Error in creating function , " + se.getMessage());
//        }
//    }

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


//    public static void HydraTask_DropFunction_subject() {
//        htsUtils = new HiveThrifServerUtils();
//        try {
//            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
//            htsUtils.snappyJDBCConnection.createStatement().execute("drop function default.subject;");
//            closeConnection(htsUtils.snappyJDBCConnection);
//            Log.getLogWriter().info("Function Dropped");
//        } catch (SQLException se) {
//            Log.getLogWriter().info("Error in dropping function , " + se.getMessage());
//        }
//    }

    public static void HydraTask_DropTableFromBeeline() {
        htsUtils = new HiveThrifServerUtils();
        htsUtils.jdbcConnection = htsUtils.connectToBeeline();
        try {
            htsUtils.jdbcConnection.createStatement().execute("drop table if exists beelineDB.Student");
            htsUtils.jdbcConnection.createStatement().execute("drop schema beelineDB restrict");
            Log.getLogWriter().info("Table beelineDB.Student dropped from beeline");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from beeline , " + se.getMessage());
        }
        closeConnection(htsUtils.jdbcConnection);
    }

    public static void HydraTask_DropTableFromSnappy() {
        htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
            htsUtils.snappyJDBCConnection.createStatement().execute("drop table if exists snappyDB.Student");
            htsUtils.snappyJDBCConnection.createStatement().execute("drop schema snappyDB restrict");
            Log.getLogWriter().info("Table snappyDB.Student dropped from snappy");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from snappy , " + se.getMessage());
        }
        closeConnection(htsUtils.snappyJDBCConnection);
    }
}
