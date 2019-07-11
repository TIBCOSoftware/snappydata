package io.snappydata.hydra.hiveThriftServer;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import java.sql.*;

public class HiveThrifServerUtils extends SnappyTest {

    private Connection jdbcConnection = null;
    private Connection snappyJDBCConnection = null;

    private Connection connectToBeeline() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.jdbcConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "app", "app");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in connecting to Beeline , " + se.getMessage());
        }
        return  htsUtils.jdbcConnection;
    }

    public static void HydraTask_CreateTableFromBeeline() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
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
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
            htsUtils.snappyJDBCConnection.createStatement().execute("create function default.subject as io.snappydata.hydra.MySubject returns string using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/subject2.jar';");
            Statement st = htsUtils.snappyJDBCConnection.createStatement();
            ResultSet rs = st.executeQuery("select default.subject(2L)");
            while(rs.next()) {
                String s = rs.getString(1);
                Log.getLogWriter().info("Function : " + s);
            }
            closeConnection(htsUtils.snappyJDBCConnection);
        } catch (SQLException se) {
            Log.getLogWriter().info("Error in creating function , " + se.getMessage());
        }
    }

    public static void HydraTask_DropFunction_subject() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
            htsUtils.snappyJDBCConnection.createStatement().execute("drop function default.subject;");
            closeConnection(htsUtils.snappyJDBCConnection);
            Log.getLogWriter().info("Function Dropped");
        } catch (SQLException se) {
            Log.getLogWriter().info("Error in creating function , " + se.getMessage());
        }
    }

    public static void HydraTask_DropTableFromBeeline() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        Connection jdbcConnection = htsUtils.connectToBeeline();
        try {
            jdbcConnection.createStatement().execute("drop table if exists default.Student");
            Log.getLogWriter().info("Table Dropped");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from beeline , " + se.getMessage());
        }
        closeConnection(jdbcConnection);
    }
}
