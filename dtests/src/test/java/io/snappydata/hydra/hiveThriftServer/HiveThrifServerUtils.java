package io.snappydata.hydra.hiveThriftServer;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

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

    private void useSchema(Connection jdbcConnection, String command) {
        try {
            jdbcConnection.createStatement().execute(command);
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in 'use schema' command , " + se.getMessage());
        }
    }

    public static void HydraTask_CreateTableFromBeeline() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        Connection jdbcConnection = htsUtils.connectToBeeline();
        htsUtils.useSchema(jdbcConnection, "use default");
        try {
            jdbcConnection.createStatement().execute("create table if not exists Student(id int, name String, subject String) using column");
            } catch(SQLException se) {
            Log.getLogWriter().info("Error in creating table from beeline , " + se.getMessage());
         }
         closeConnection(jdbcConnection);
    }

    public static void HydraTask_CreateFunction() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
//            htsUtils.snappyJDBCConnection.createStatement().execute("use default");
            htsUtils.snappyJDBCConnection.createStatement().execute("create function default.subject as io.snappydata.hydra.MySubject returns string using jar '/home/cbhatt/TestWork/out/artifacts/TestWork_jar/TestWork.jar';");
            closeConnection(htsUtils.snappyJDBCConnection);
        } catch (SQLException se) {
            Log.getLogWriter().info("Error in creating function , " + se.getMessage());
        }
    }

    public static void HydraTask_DropFunction() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        try {
            htsUtils.snappyJDBCConnection = SnappyTest.getLocatorConnection();
//            htsUtils.snappyJDBCConnection.createStatement().execute("use default");
            htsUtils.snappyJDBCConnection.createStatement().execute("drop function default.subject;");
            closeConnection(htsUtils.snappyJDBCConnection);
        } catch (SQLException se) {
            Log.getLogWriter().info("Error in creating function , " + se.getMessage());
        }
    }


    public static void HydraTask_DropTableFromBeeline() {
        HiveThrifServerUtils htsUtils = new HiveThrifServerUtils();
        Connection jdbcConnection = htsUtils.connectToBeeline();
        htsUtils.useSchema(jdbcConnection, "use default");
        try {
            jdbcConnection.createStatement().execute("drop table if exists Student");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from beeline , " + se.getMessage());
        }
        closeConnection(jdbcConnection);
    }
}
