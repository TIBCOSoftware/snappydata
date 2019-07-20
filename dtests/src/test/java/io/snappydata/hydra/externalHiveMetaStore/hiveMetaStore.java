package io.snappydata.hydra.externalHiveMetaStore;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;

import java.sql.*;

public class hiveMetaStore extends SnappyTest
{
    Connection connection = null;
    ResultSet rs = null;
    Statement st = null;
    ResultSetMetaData rsmd = null;

    static hiveMetaStore metaStore = new hiveMetaStore();


    public hiveMetaStore() {
    }

    public static void HydraTask_Wait() {
        try {
            Log.getLogWriter().info("Waiting for 30 seconds");
            Thread.sleep(30000);
        } catch(InterruptedException e) {
            Log.getLogWriter().info(e.getMessage());
        }
    }

    public static void HydraTask_ConnectToBeeline() {
        metaStore.connectToBeeline(metaStore);
    }

    public static  void HydraTask_DisconnectBeeline(){
        metaStore.disconnectToBeeline(metaStore);
    }

    public static void HydraTask_Test() {
        try {
            metaStore.st = metaStore.connection.createStatement();
            metaStore.rs = metaStore.st.executeQuery("show tables");
            while(metaStore.rs.next()) {
                Log.getLogWriter().info("Result : " + metaStore.rs.getString(1));
            }
        }catch(SQLException se) {

        }
    }

    private  void connectToBeeline(hiveMetaStore metaStore) {
        try {
            metaStore.connection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "APP", "mine");
            Log.getLogWriter().info("Connection to beeline is successful...");
        }catch (SQLException se) {
            throw new TestException("Got the exception while connecting to beeline...,"  + " " + se.getMessage());
        }
    }

    private  void disconnectToBeeline(hiveMetaStore metaStore) {
        try{
                metaStore.connection.close();
                if(metaStore.connection.isClosed()) {
                    Log.getLogWriter().info("Connection with beeline successfully CLOSED...");
                }
        } catch(SQLException se) {
            throw new TestException("Got the exception whild disconnecting to beeline, " + se.getMessage());
        }

    }
}
