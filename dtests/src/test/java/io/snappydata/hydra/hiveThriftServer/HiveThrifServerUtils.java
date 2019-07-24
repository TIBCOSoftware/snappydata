package io.snappydata.hydra.hiveThriftServer;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsBB;
import io.snappydata.hydra.testDMLOps.SnappyDMLOpsUtil;
import org.apache.commons.lang.StringUtils;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class HiveThrifServerUtils extends SnappyTest {

    public static HiveThrifServerUtils htsUtils = null;

    public static void HydraTask_initialize(){
        if(htsUtils==null)
            htsUtils = new HiveThrifServerUtils();
    }

    public static String[] beelineQuery = {"update beelineDB.Student set marks = marks + 100",
            "update beelineDB.Student set subject = 'Multithreading and Concurrency' where subject='Maths-2'",
            "update beelineDB.Student set marks = marks - 200",
            "update beelineDB.Student set subject = 'Maths-2' where subject='Multithreading and Concurrency'",
            "delete from beelineDB.Student where subject = 'Graphics' "};

    public static String[] snappyQuery = {"update snappyDB.Student set marks = marks + 100",
            "update snappyDB.Student set subject = 'Multithreading and Concurrency' where subject='Maths-2'",
            "update snappyDB.Student set marks = marks - 200",
            "update snappyDB.Student set subject = 'Maths-2' where subject='Multithreading and Concurrency'",
            "delete from snappyDB.Student where subject = 'Graphics'"};

    private static Connection connectToBeeline() {
        Connection jdbcConnection = null;
        try {
            jdbcConnection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "app", "app");
        } catch(SQLException se) {
            throw new TestException("Error when connecting  to Beeline", se);
        }
        return  jdbcConnection;
    }

    public static void HydraTask_CreateTableFromBeeline() {
        Connection jdbcConnection = connectToBeeline();
        try {
            Vector filePath = SnappyPrms.getDataLocationList();
            Log.getLogWriter().info("Hive Thrift Server data path : " + filePath.firstElement().toString());
            jdbcConnection.createStatement().execute("create schema beelineDB");
            //Below line needs to be added when external hive meta store merged into the master.
            //htsUtils.jdbcConnection.createStatement().execute("create table if not exists beelineDB.Student(id int, name String, subject String, marks int, tid int) row format delimited fields terminated by ','");
            //htsUtils.jdbcConnection.createStatement().execute("load data local inpath '/home/cbhatt/hts1/' overwrite into table beelineDB.Student");
            jdbcConnection.createStatement().execute("create external table if not exists beelineDB.stage_Student using csv options(path '" + filePath.firstElement().toString() + "',header 'true', inferSchema 'false',nullValue 'NULL', maxCharsPerColumn '4096')");
            jdbcConnection.createStatement().execute("create table beelineDB.Student(id int,name string,subject string,marks int,tid int) using column as select * from beelineDB.stage_Student");
            Log.getLogWriter().info("Table beelineDB.Student created successfully in beeline");
            Statement st = jdbcConnection.createStatement();
            ResultSet rs = st.executeQuery("select * from beelineDB.Student limit 10");
            while (rs.next()) {
                Log.getLogWriter().info(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5) );
            }
            } catch(SQLException se) {
            throw new TestException("Error in creating table from beeline ,", se);
        }
         closeConnection(jdbcConnection);
    }

    public static void HydraTask_CreateTableFromSnappy() {
        Connection snappyJDBCConnection = null;
        try {
            Vector filePath = SnappyPrms.getDataLocationList();
            Log.getLogWriter().info("Hive Thrift Server data path : " + filePath.firstElement().toString());
            snappyJDBCConnection = SnappyTest.getLocatorConnection();
            snappyJDBCConnection.createStatement().execute("create schema snappyDB");
            snappyJDBCConnection.createStatement().execute("create external table if not exists snappyDB.stage_Student using csv options(path '" + filePath.firstElement().toString()  + "',header 'true', inferSchema 'false',nullValue 'NULL', maxCharsPerColumn '4096')");
            snappyJDBCConnection.createStatement().execute("create table snappyDB.Student(id int,name string,subject string,marks int,tid int) using column as select * from snappyDB.stage_Student");
            Log.getLogWriter().info("Table snappyDB.Student created successfully in snappy");
            Statement st = snappyJDBCConnection.createStatement();
            ResultSet rs = st.executeQuery("select * from snappyDB.Student limit 10");
            while (rs.next()) {
                Log.getLogWriter().info(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5) );
            }

            Log.getLogWriter().info("Table snappyDB.Student created successfully in Snappy");
        } catch(SQLException se) {
            throw new TestException("Error in creating table from snappy ", se);
        }
        closeConnection(snappyJDBCConnection);
    }


    public static void HydraTask_verfiyLoad() {

        SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();

        String logFile = getCurrentDirPath() + "/resultdir";

        File queryResultDir = new File(logFile);
        if (!queryResultDir.exists())
            queryResultDir.mkdirs();

        String beelineFile, snappyFile;

        int tid = htsUtils.getMyTid();
        Log.getLogWriter().info("tid --> " + tid);

        ResultSet snappyRs = null;
        ResultSet beelineRs = null;

        Connection snappyJDBCConnection = null, jdbcConnection = null;
        try {
            snappyJDBCConnection = SnappyTest.getLocatorConnection();
            jdbcConnection = connectToBeeline();
            Log.getLogWriter().info("SnappyConnection : " + snappyJDBCConnection.isClosed());
            Log.getLogWriter().info("JDBCConnection : " + jdbcConnection.isClosed());
        } catch (SQLException se) {
            throw new TestException("Error while establishing the connection : " , se);
        }

        try {
        beelineRs = jdbcConnection.createStatement().executeQuery("select * from beelineDB.Student ");
        snappyRs = snappyJDBCConnection.createStatement().executeQuery("select * from snappyDB.Student");

        StructTypeImpl beelineSti = ResultSetHelper.getStructType(beelineRs);
        StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRs);
        List<Struct> beelineList = ResultSetHelper.asList(beelineRs, beelineSti, false);
        List<Struct> snappyList = ResultSetHelper.asList(snappyRs, snappySti, false);
        beelineRs.close();
        snappyRs.close();

        beelineFile = logFile + File.separator + "beelineQuery_" + tid +  ".out";
        snappyFile = logFile + File.separator + "snappyQuery_" + tid +  ".out";

        testInstance.listToFile(snappyList, snappyFile);
        testInstance.listToFile(beelineList,beelineFile);

        String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_"  + tid);
        if(msg.length() > 0 ) {
            throw new TestException("Validation failed : " + msg);
        }
    } catch (SQLException se) {
        Log.getLogWriter().info("Error while closing the connection, " + se.getMessage());
    }
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
        SnappyDMLOpsUtil testInstance = new SnappyDMLOpsUtil();

        String logFile = getCurrentDirPath() + "/resultdir";

        File queryResultDir = new File(logFile);
        if (!queryResultDir.exists())
            queryResultDir.mkdirs();

        String beelineFile, snappyFile;

        int tid = htsUtils.getMyTid();
        Log.getLogWriter().info("tid --> " + tid);

        ResultSet snappyRs = null;
        ResultSet beelineRs = null;

        Connection snappyJDBCConnection = null, jdbcConnection = null;
        try {
            snappyJDBCConnection = SnappyTest.getLocatorConnection();
            jdbcConnection = connectToBeeline();
            Log.getLogWriter().info("SnappyConnection : " + snappyJDBCConnection.isClosed());
            Log.getLogWriter().info("JDBCConnection : " + jdbcConnection.isClosed());
        } catch (SQLException se) {
           throw new TestException("Error while establishing the connection :  " , se);
        }
        for (int index = 0; index < snappyQuery.length; index++) {

            String beelineStmt = beelineQuery[index];
            Log.getLogWriter().info("tid = " + tid);
            if (beelineStmt.toUpperCase().contains("WHERE"))
                beelineStmt = beelineStmt + " AND tid = " + tid;
            else
                beelineStmt = beelineStmt + " WHERE tid = " + tid;
            String snappyStmt = snappyQuery[index];
            if (snappyStmt.toUpperCase().contains("WHERE"))
                snappyStmt = snappyStmt + " AND tid = " + tid;
            else
                snappyStmt = snappyStmt + " WHERE tid = " + tid;

            Log.getLogWriter().info("beelineQuery : " + beelineStmt);
            Log.getLogWriter().info("snappyQuery :  " + snappyStmt);

            try {
                jdbcConnection.createStatement().executeUpdate(beelineStmt);
                snappyJDBCConnection.createStatement().executeUpdate(snappyStmt);

                beelineRs = jdbcConnection.createStatement().executeQuery("select * from beelineDB.Student where tid = " + tid);
                snappyRs = snappyJDBCConnection.createStatement().executeQuery("select * from snappyDB.Student where tid = " + tid);

                StructTypeImpl beelineSti = ResultSetHelper.getStructType(beelineRs);
                StructTypeImpl snappySti = ResultSetHelper.getStructType(snappyRs);
                List<Struct> beelineList = ResultSetHelper.asList(beelineRs, beelineSti, false);
                List<Struct> snappyList = ResultSetHelper.asList(snappyRs, snappySti, false);
                beelineRs.close();
                snappyRs.close();

                beelineFile = logFile + File.separator + "beelineQuery_" + index + "_" + tid + ".out";
                snappyFile = logFile + File.separator + "snappyQuery_" + index + "_" + tid + ".out";

                testInstance.listToFile(snappyList, snappyFile);
                testInstance.listToFile(beelineList, beelineFile);

                String msg = testInstance.compareFiles(logFile, beelineFile, snappyFile, false, "query_" + index);
                if (msg.length() > 0) {
                    throw new TestException("Validation failed : " + msg);
                }
            } catch(SQLException se) {
                throw new TestException("Got Exception while executing stmt", se);
            }
        }

        try {
            jdbcConnection.close();
            snappyJDBCConnection.close();
        } catch (SQLException se) {
            Log.getLogWriter().info("Error while closing the connection, " + se.getMessage());
        }

    }

    public static void HydraTask_DropTableFromBeeline() {
        Connection jdbcConnection = connectToBeeline();
        try {
            jdbcConnection.createStatement().execute("drop table if exists beelineDB.stage_Student");
            jdbcConnection.createStatement().execute("drop table if exists beelineDB.Student");
            jdbcConnection.createStatement().execute("drop schema beelineDB restrict");
            Log.getLogWriter().info("Table beelineDB.Student dropped from beeline");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from beeline , " + se.getMessage());
        }
        closeConnection(jdbcConnection);
    }

    public static void HydraTask_DropTableFromSnappy() {
        Connection snappyJDBCConnection = null;
        try {
            snappyJDBCConnection = SnappyTest.getLocatorConnection();
            snappyJDBCConnection.createStatement().execute("drop table if exists snappyDB.stage_Student");
            snappyJDBCConnection.createStatement().execute("drop table if exists snappyDB.Student");
            snappyJDBCConnection.createStatement().execute("drop schema snappyDB restrict");
            Log.getLogWriter().info("Table snappyDB.Student dropped from snappy");
        } catch(SQLException se) {
            Log.getLogWriter().info("Error in droping table from snappy , " + se.getMessage());
        }
        closeConnection(snappyJDBCConnection);
        Log.getLogWriter().info("HiveThrift Server Concurrent validation test successful.");
    }
}
