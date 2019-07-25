package io.snappydata.hydra.externalHiveMetaStore;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;

import java.sql.*;

public class hiveMetaStore extends SnappyTest
{
    Connection beelineConnection = null;
    Connection snappyConnection = null;
    ResultSet rs = null;
    Statement st = null;
    ResultSet snappyRS = null;
    Statement snappyST = null;
    ResultSetMetaData rsmd = null;

    static String setexternalHiveCatalog = "set spark.sql.catalogImplementation=hive";
    static String setexternalInBuiltCatalog = "set spark.sql.catalogImplementation=in-memory";
    static String showTblsApp = "show tables in app";
    static String showTblsDefault = "show tables in default";

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
        try {
            metaStore.beelineConnection = DriverManager.getConnection("jdbc:hive2://localhost:11000", "APP", "mine");
            Log.getLogWriter().info("Connection to beeline is successful...");
        }catch (SQLException se) {
            throw new TestException("Got the exception while connecting to beeline...,"  + " " + se.getMessage());
        }
    }

    public static  void HydraTask_DisconnectBeeline(){
        try {
            metaStore.beelineConnection.close();
            if(metaStore.beelineConnection.isClosed()) {
                Log.getLogWriter().info("Connection with beeline successfully CLOSED...");
            }
        } catch(SQLException se) {
            throw new TestException("Got the exception whild disconnecting to beeline",se);
        }
    }

    public static void HydraTask_ConnectToSnappy() {
        try {
            metaStore.snappyConnection = SnappyTest.getLocatorConnection();
            Log.getLogWriter().info("Connection to Snappy is successful...");
        }catch (SQLException se) {
            throw new TestException("Got the exception while connecting to Snappy", se);
        }
    }

    public static  void HydraTask_DisconnectSnappy(){
        try {
            metaStore.snappyConnection.close();
            if(metaStore.snappyConnection.isClosed()) {
                Log.getLogWriter().info("Connection to Snappy successfully CLOSED...");
            }
        } catch(SQLException se) {
            throw new TestException("Got the exception while disconnecting to Snappy",se);
        }
    }

    public static void  HydraTask_TestBeeline() {
        try {
            metaStore.st = metaStore.beelineConnection.createStatement();
            metaStore.rs = metaStore.st.executeQuery(showTblsDefault);
            while(metaStore.rs.next()) {
                Log.getLogWriter().info("Beeline Result : " + metaStore.rs.getString(1));
            }
        }catch(SQLException se) {
            throw new TestException("Got the exception for show tables command from beeline", se);
        }
    }

    public static void  HydraTask_TestSnappy() {
        try {
            metaStore.snappyConnection.createStatement().execute(setexternalHiveCatalog);
            Log.getLogWriter().info("Pointing to external hive meta store");
            metaStore.snappyST = metaStore.snappyConnection.createStatement();
            Log.getLogWriter().info("create statement from snappy");
            metaStore.snappyRS = metaStore.metaStore.snappyST.executeQuery(showTblsDefault);
            Log.getLogWriter().info("execute show tables in default");
            while(metaStore.snappyRS.next()) {
                Log.getLogWriter().info("Snappy Result : " + metaStore.snappyRS.getString(1) + " | " + metaStore.snappyRS.getString(2 ));
            }
        }catch(SQLException se) {
            throw new TestException("Got the exception for show tables command from Snappy", se);
        }
    }

//    private  void connectToBeeline(hiveMetaStore metaStore) {
//        try {
//            metaStore.beelineConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "APP", "mine");
//            Log.getLogWriter().info("Connection to beeline is successful...");
//        }catch (SQLException se) {
//            throw new TestException("Got the exception while connecting to beeline...,"  + " " + se.getMessage());
//        }
//    }
//
//    private  void disconnectToBeeline(hiveMetaStore metaStore) {
//        try {
//                metaStore.beelineConnection.close();
//                if(metaStore.beelineConnection.isClosed()) {
//                    Log.getLogWriter().info("Connection with beeline successfully CLOSED...");
//                }
//        } catch(SQLException se) {
//            throw new TestException("Got the exception whild disconnecting to beeline, " + se.getMessage());
//        }
//
//    }
}
