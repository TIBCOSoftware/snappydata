package io.snappydata.hydra.cluster;


import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.query.facets.lang.Course;
import hydra.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import util.PRObserver;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;

import static java.lang.Thread.sleep;

public class SnappyTest implements Serializable {

    private static transient JavaSparkContext jsc;
    private static transient SnappyContext snc;
    protected static SnappyTest snappyTest;
    private static char sep;
    private static HostDescription hd;
    private static String columnTable = "AIRLINE";
    private static String rowTable = "AIRLINEREF";
    private static String stagingColumnTable = "STAGING_AIRLINE";
    private static String stagingRowTable = "STAGING_AIRLINEREF";
    private static String snappyPath = null;
    private static String logFile = null;
    private static String localHost = null;

    private static Vector<String> clentNames = TestConfig.getInstance().getClientNames();
    private static Set<String> serversFileContent = new LinkedHashSet<String>();
    private static Set<String> locatorsFileContent = new LinkedHashSet<String>();
    private static Set<String> leadsFileContent = new LinkedHashSet<String>();
    private static Set<Integer> pids = new LinkedHashSet<Integer>();
    private static String locatorsFilePath = null;
    private static String serversFilePath = null;
    private static String leadsFilePath = null;
    protected static boolean cycleVms = TestConfig.tab().booleanAt(SnappyPrms.cycleVms, true);
    protected static boolean streamingJob = TestConfig.tab().booleanAt(SnappyPrms.streamingJob, false);
    private static String userAppJar = TestConfig.tab().stringAt(SnappyPrms.userAppJar, "gemfirexd-scala-tests-0.1.0-SNAPSHOT-tests.jar");
    private static String simulateStreamScriptName = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptName, "simulateFileStream");
    private static String simulateStreamScriptDestinationFolder = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptDestinationFolder, "/home/swati");
    public static final String LASTCYCLEDTIME = "lastCycledTime"; //used in SnappyBB
    private static String leadHost = null;
    public static long lastCycledTime = 0;
    public static int waitTimeBeforeNextCycleVM = TestConfig.tab().intAt(SnappyPrms.waitTimeBeforeNextCycleVM, 20); //secs
    public static Long waitTimeBeforeJobStatusInTask = TestConfig.tab().longAt(SnappyPrms.snappyJobExecutionTimeInMillisForTask, 600000);
    public static Long waitTimeBeforeJobStatusInCloseTask = TestConfig.tab().longAt(SnappyPrms.snappyJobExecutionTimeInMillisForCloseTask, 600000);
    public static final int THOUSAND = 1000;
    public static String cycleVMTarget = TestConfig.tab().stringAt(SnappyPrms.cycleVMTarget, "store");
    private static Set active;

    public static <A, B> Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>>conforms());
    }

    public static void HydraTask_initializeSnappy() {
/*        org.apache.spark.SparkConf conf = new org.apache.spark.SparkConf().setAppName("SnappyTest").setMaster("local").set("spark.ui.port", "6060");
        jsc = new JavaSparkContext(conf);
        snc = SnappyContext.getOrCreate(JavaSparkContext.toSparkContext(jsc));*/
        SparkContext sc = null;
        snc = SnappyContext.getOrCreate(sc);
        Log.getLogWriter().info("SS - SnappyContext initialized successfully");
    }


    public static void HydraTask_stopSnappy() {
        snc.stop();
        Log.getLogWriter().info("SS - SnappyContext stopped successfully");

    }

    public static void HydraTask_createRowColumnTableUsingSQL() {
        HydraTask_initializeSnappyTest();
        String hfile = snappyTest.getDataLocation() + "/airlineParquetData";
        String reffile = snappyTest.getDataLocation() + "/airportcodeParquetData";

        snc.sql("CREATE TABLE " + stagingColumnTable + " \n" +
                " USING parquet OPTIONS(path '" + hfile + "')");
        Log.getLogWriter().info("SS - staging column table " + stagingColumnTable + " created successfully");
        snc.sql("CREATE TABLE " + columnTable + " USING column OPTIONS(buckets '11') AS (\n" +
                "  SELECT Year AS yeari, Month AS monthi , DayOfMonth,\n" +
                "    DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,\n" +
                "    UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,\n" +
                "    CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,\n" +
                "    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,\n" +
                "    Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,\n" +
                "    LateAircraftDelay, ArrDelaySlot\n" +
                "    FROM " + stagingColumnTable + ")");
        Log.getLogWriter().info("SS - column table " + columnTable + " created successfully");
        snc.sql("CREATE TABLE " + stagingRowTable + " \n" +
                "  USING parquet OPTIONS(path '" + reffile + "')");
        Log.getLogWriter().info("SS - staging row table " + stagingRowTable + " created successfully");
        snc.sql("CREATE TABLE " + rowTable + " USING row OPTIONS() AS (SELECT CODE, DESCRIPTION FROM " + stagingRowTable + ")");
        Log.getLogWriter().info("SS - row table " + rowTable + " created successfully");
    }


    public static void HydraTask_createRowColumnTableUsingDataFrame() {
        HydraTask_initializeSnappyTest();
        String hfile = snappyTest.getDataFrameDataLocation() + "/2015.parquet";
        String reffile = snappyTest.getDataLocation() + "/airportcodeParquetData";
        DataFrame airlineDataFrame, airlineRefDataFrame;
        HashMap<String, String> props = new HashMap<String, String>();
//        props.put("buckets","11");
        airlineDataFrame = snc.read().load(hfile);
        snc.read().load(hfile).registerTempTable(stagingColumnTable);
        Log.getLogWriter().info("SS - airline dataframe loaded successfully");
        snc.dropTable(columnTable, true);
        snc.createTable(columnTable, "column", airlineDataFrame.schema(), toScalaMap(props));
        airlineDataFrame.write().format("column").mode(SaveMode.Append).saveAsTable(columnTable);
        Log.getLogWriter().info("SS - column table " + columnTable + " created successfully");
        airlineRefDataFrame = snc.read().load(reffile);
        snc.read().load(reffile).registerTempTable(stagingRowTable);
        props.clear();
        Log.getLogWriter().info("SS - airlineref dataframe loaded successfully");
        snc.dropTable(rowTable, true);
        snc.createTable(rowTable, "row", airlineRefDataFrame.schema(), toScalaMap(props));
        airlineRefDataFrame.write().format("row").mode(SaveMode.Append).saveAsTable(rowTable);
        Log.getLogWriter().info("SS - row table " + rowTable + " created successfully");
    }

    public static void HydraTask_dropRowColumnTable() {
        snc.dropTable(stagingColumnTable, true);
        snc.dropTable(columnTable, true);
        snc.dropTable(stagingRowTable, true);
        snc.dropTable(rowTable, true);
    }

    /**
     * This method gives simple count(*) query results on airline table.
     */

    public static void HydraTask_countQueryOnColumnTable() {
        String query1 = "SELECT count(*) FROM airline";
        Row[] result = snc.sql(query1).collect();
        Assert.assertEquals(1, result.length);
        Long expectedValue = Long.valueOf(1000000);
        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }


    /**
     * This method retrives which airline had the most flights each year.
     */

    public static void HydraTask_countQueryWithGroupByOrderBy() {
        String query = "select  count(*) flightRecCount, description AirlineName, UniqueCarrier carrierCode ,yeari \n" +
                "   from airline , airlineref\n" +
                "   where airline.UniqueCarrier = airlineref.code\n" +
                "   group by UniqueCarrier,description, yeari \n" +
                "   order by flightRecCount desc limit 10 ";
        Row[] result = snc.sql(query).collect();
        Long expectedValue = Long.valueOf(190214);
        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives which Airlines Arrive On Schedule
     */

    public static void HydraTask_avgArrDelayWithGroupByOrderByForSchedule() {
        String query = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from airline   \n" +
                "   group by UniqueCarrier\n" +
                "   order by arrivalDelay ";
        Row[] result = snc.sql(query).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives which Airlines Arrive On Schedule. JOIN with reference table.
     */

    public static void HydraTask_avgArrDelayWithGroupByOrderByWithJoinForSchedule() {
        String query = "select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier \n" +
                "  from airline, airlineref\n" +
                "  where airline.UniqueCarrier = airlineref.Code \n" +
                "  group by UniqueCarrier, description \n" +
                "  order by arrivalDelay ";
        Row[] result = snc.sql(query).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives the trend in arrival delays across all airlines in the US
     */

    public static void HydraTask_avgArrDelayWithGroupByOrderByForTrendAnalysis() {
        String query = "select AVG(ArrDelay) ArrivalDelay, yeari\n" +
                "  from airline \n" +
                "  group by yeari \n" +
                "  order by yeari ";
        Row[] result = snc.sql(query).collect();
//        Long expectedValue = Long.parseLong("6.735443");
//        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method retrives Which airline out of SanFrancisco had most delays due to weather
     */

    public static void HydraTask_sumWeatherDelayWithGroupByWithLimit() {
        String query = "SELECT sum(WeatherDelay) totalWeatherDelay, airlineref.DESCRIPTION \n" +
                "  FROM airline, airlineref \n" +
                "  WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 \n" +
                "  GROUP BY DESCRIPTION \n" +
                "  limit 20 ";
        Row[] result = snc.sql(query).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result[0]).get(0));
    }

    /**
     * This method updates the description for a code in row table. Suppose a particular Airline company say 'Delta Air Lines Inc.' re-brands itself as 'Delta America'
     * --- the airline code can be updated in the row table
     */

    public static void HydraTask_updateRowTable() {
        String query1 = "UPDATE AIRLINEREF SET DESCRIPTION='Delta America' WHERE CODE='DL' ";
        Row[] result = snc.sql(query1).collect();
        Log.getLogWriter().info("SS - Qyery executed successfully");
        String query2 = "SELECT * FROM AIRLINEREF WHERE CAST(CODE AS VARCHAR(25))='DL'";
        Row[] result1 = snc.sql(query2).collect();
//        Long expectedValue = Long.parseLong("DL");
//        Assert.assertEquals(expectedValue, ((GenericRow) result[0]).get(0));
        Log.getLogWriter().info("SS - Qyery executed successfully and query result is ::" + ((GenericRow) result1[0]).get(0));
    }

    protected String getDataLocation() {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "quickstart" + sep + "data";
            Log.getLogWriter().info("SS - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            throw e;
        }
        return scriptPath;
    }

    protected String getDataFrameDataLocation() {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + ".."
                    + sep + ".." + sep + "tests-common" + sep + "src" + sep + "main" + sep + "resources";
            Log.getLogWriter().info("SS - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            throw e;
        }
        return scriptPath;
    }

    protected HostDescription getHostDescription() {
        hd = TestConfig.getInstance()
                .getClientDescription(RemoteTestModule.getMyClientName())
                .getVmDescription().getHostDescription();
        sep = hd.getFileSep();
        Log.getLogWriter().info("SS - Inside getHostDescription - gemfireHome is ::" + hd.getGemFireHome());
        return hd;
    }

    protected String getScriptLocation(String scriptName) {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "sbin" + sep + scriptName;
            Log.getLogWriter().info("SS - Inside getScriptLocation - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            throw e;
        }
        return scriptPath;
    }

    protected String getSnappyPath() {
        hd = getHostDescription();
        snappyPath = hd.getGemFireHome() + sep + ".." + sep + "snappy";
        Log.getLogWriter().info("SS - Inside getSnappyPath - snappyPath is ::" + snappyPath);
        return snappyPath;
    }

    protected String getUserScriptLocation(String scriptName) {
        String scriptPath = null;
        try {
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + ".." + sep + ".." + sep + "snappy-dtests" + sep + "src" + sep + "resources" + sep + "scripts" + sep + scriptName;
            Log.getLogWriter().info("SS - Inside getUserScriptLocation - scriptPath is ::" + scriptPath);
        } catch (Exception e) {
            throw e;
        }
        return scriptPath;
    }

    protected String getUserAppJarLocation(String userAppJar) {
        String jarPath = null;
        try {
            jarPath = hd.getGemFireHome() + sep + ".." + sep + ".." + sep + ".." + sep + "snappy-dtests" + sep + "build-artifacts" + sep + "scala-2.10" + sep + "libs" + sep + userAppJar;
            Log.getLogWriter().info("SS - Inside getUserAppJarLocation - jarPath is ::" + jarPath);
        } catch (Exception e) {
            throw e;
        }
        return jarPath;
    }

    public static void HydraTask_initializeSnappyTest() {
        if (snappyTest == null) {
            snappyTest = new SnappyTest();
            snappyTest.generateConfig("locators");
            snappyTest.generateConfig("servers");
            snappyTest.generateConfig("leads");
        }
    }

    public static synchronized void HydraTask_generateSnappyConfig() {
        locatorsFilePath = snappyTest.getSnappyPath() + sep + "conf" + sep + "locators";
        serversFilePath = snappyTest.getSnappyPath() + sep + "conf" + sep + "servers";
        leadsFilePath = snappyTest.getSnappyPath() + sep + "conf" + sep + "leads";
        try {
            Log.getLogWriter().info("Inside generateSnappyConfig...");
            String dirPath = snappyTest.getLogDir();
            File locatorsFile = new File(locatorsFilePath);
            File serversFile = new File(serversFilePath);
            File leadsFile = new File(leadsFilePath);
            if (dirPath.contains("locator")) {
                String locatorLogDir = localHost + " -dir=" + dirPath;
                if (!locatorsFileContent.contains(locatorLogDir)) {
                    locatorsFileContent.add(locatorLogDir);
                    snappyTest.writeToFile(locatorLogDir, locatorsFile);
                }
            } else if (dirPath.contains("Store")) {
                String serverLogDir = localHost + " -dir=" + dirPath;
                if (!serversFileContent.contains(serverLogDir)) {
                    serversFileContent.add(serverLogDir);
                    snappyTest.writeToFile(serverLogDir, serversFile);
                }
            } else if (dirPath.contains("lead")) {
                String leadLogDir = localHost + " -dir=" + dirPath;
                if (leadHost == null) {
                    leadHost = localHost;
                }
                Log.getLogWriter().info("Lead host is: " + leadHost);
                if (!leadsFileContent.contains(leadLogDir)) {
                    leadsFileContent.add(leadLogDir);
                    snappyTest.writeToFile(leadLogDir, leadsFile);
                }
            }
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getStackTrace());
        }
        Log.getLogWriter().info("Done with generateSnappyConfig...");
    }

    protected void writeToFile(String logDir, File file) {
        try {
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(logDir);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }

    /**
     * Executes user scripts for InitTask.
     */

    public static void HydraTask_executeSQLScriptsInInitTask() {
        Vector scriptNames = null;
        String fileName = "initTask.sql";
        scriptNames = SnappyPrms.getSQLScriptNamesForInitTask();
        Log.getLogWriter().info("scriptSize is :" + scriptNames.size());
        Log.getLogWriter().info("scriptNames are :" + scriptNames.toString());
        try {
            String SnappyShellPath = snappyTest.getSnappyPath() + sep + "bin" + sep + "snappy-shell";
            File file = snappyTest.writeDataToSQLFile(scriptNames, fileName);
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "sqlScriptsInitTaskResult.log";
            File logFile = new File(dest);
            ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" + file);
            snappyTest.executeProcess(pb, logFile);
        } catch (Exception e1) {
            snappyTest.removeSnappyProcessIDinNukeRun();
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected void executeProcess(ProcessBuilder pb, File logFile) {
        try {
            Process p = null;
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
            p = pb.start();
            Log.getLogWriter().info("SS - snappy-shell started successfully");
            assert pb.redirectInput() == ProcessBuilder.Redirect.PIPE;
            assert pb.redirectOutput().file() == logFile;
            assert p.getInputStream().read() == -1;
            int rc = p.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected void recordSnappyProcessIDinNukeRun(String pName) {
        try {
            String command = "ps ax | grep " + pName + " | grep -v grep | awk '{print $1}'";
            hd = TestConfig.getInstance().getMasterDescription()
                    .getVmDescription().getHostDescription();
            Log.getLogWriter().info("Host name retrieved from recordSnappyProcessIDinNukeRun::" + hd.getHostName());
            ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "PIDs.log";
            File logFile = new File(dest);
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
            Process pr = pb.start();
            pr.waitFor();
            FileInputStream fis = new FileInputStream(logFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String str = null;
            while ((str = br.readLine()) != null) {
                Log.getLogWriter().info("pid value in recordSnappyProcessIDinNukeRun is :: " + str);
                int pid = Integer.parseInt(str);
                try {
                    if (pids.contains(pid)) {
                        Log.getLogWriter().info("Pid is already recorded with Master" + pid);
                    } else {
//                        SnappyBB.getBB().getSharedMap().put("PIDs", pid);
                        pids.add(pid);
                        RemoteTestModule.Master.recordPID(hd, pid);
                    }
                } catch (RemoteException e) {
                    String s = "Unable to access master to record PID: " + pid;
                    throw new HydraRuntimeException(s, e);
                }
                Log.getLogWriter().info("pid value successfully recorded with Master");
            }
            br.close();
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }

    protected void removeSnappyProcessIDinNukeRun() {
        try {
            hd = TestConfig.getInstance().getMasterDescription()
                    .getVmDescription().getHostDescription();
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "PIDs.log";
            File pidFile = new File(dest);
            FileInputStream fis = new FileInputStream(pidFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String str = null;
            while ((str = br.readLine()) != null) {
                Log.getLogWriter().info("pid value in removeSnappyProcessIDinNukeRun is :: " + str);
                int pid = Integer.parseInt(str);
                try {
                    RemoteTestModule.Master.removePID(hd, pid);
                } catch (RemoteException e) {
                    String s = "Failed to remove PID from nukerun script: " + pid;
                    throw new HydraRuntimeException(s, e);
                }
                Log.getLogWriter().info("pid value successfully removed from nukerun script");
            }
            br.close();
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }


    /**
     * Executes user scripts for InitTask.
     */

    public static void HydraTask_executeSQLScriptsInTask() {
        Vector scriptNames = null;
        String fileName = "task.sql";
        scriptNames = SnappyPrms.getSQLScriptNamesForTask();
        Log.getLogWriter().info("scriptSize is :" + scriptNames.size());
        Log.getLogWriter().info("scriptNames are :" + scriptNames.toString());
        try {
            String SnappyShellPath = snappyTest.getSnappyPath() + sep + "bin" + sep + "snappy-shell";
            File file = snappyTest.writeDataToSQLFile(scriptNames, fileName);
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "sqlScriptsTaskResult.log";
            File logFile = new File(dest);
            ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" + file);
            snappyTest.executeProcess(pb, logFile);
        } catch (Exception e1) {
            snappyTest.removeSnappyProcessIDinNukeRun();
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }

    }

    /**
     * Executes snappy Jobs in Close Task.
     */

    public static void HydraTask_executeSnappyJobInCloseTask() {
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNamesForCloseTask(), "snappyJobCloseTaskResult.log");
        try {
            sleep(waitTimeBeforeJobStatusInCloseTask);
        } catch (InterruptedException e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }

    /**
     * Executes snappy Jobs in Task.
     */

    public static void HydraTask_executeSnappyJobInTask() {
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNamesForTask(), "snappyJobTaskResult.log");
    }

    /**
     * Executes snappy Streaming Jobs in Task.
     */

    public static void HydraTask_executeSnappyStreamingJob() {
        Log.getLogWriter().info("Entered for streaming job execution .....");
        Runnable fileStreaming = new Runnable() {
            public void run() {
                try {
                    Log.getLogWriter().info("Entered for executeSnappyStreamingJob .....");
                    snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyJobClassNamesForTask(), "snappyJobTaskResult.log");
                } catch (Exception e) {
                    throw new TestException("Following error occurred while executing " + e.getMessage());
                }
            }
        };

        Runnable simulateFileStream = new Runnable() {
            public void run() {
                try {
                    snappyTest.simulateStream();
                } catch (Exception e) {
                    throw new TestException("Following error occurred while executing " + e.getMessage());
                }
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(fileStreaming);
        es.submit(simulateFileStream);
        try {
            Log.getLogWriter().info("Sleeping for " + waitTimeBeforeJobStatusInTask + "millis before executor service shut down");
            Thread.sleep(waitTimeBeforeJobStatusInTask);
            Log.getLogWriter().info("done Sleeping for " + waitTimeBeforeJobStatusInTask + "millis before executor service shut down");
            es.shutdown();
            es.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }

    protected void executeSnappyStreamingJob(Vector jobClassNames, String logFileName) {
        String snappyJobScript = snappyTest.getSnappyPath() + sep + "bin" + sep + "snappy-job.sh";
        ProcessBuilder pb = null;
        File logFile = null;
        Log.getLogWriter().info("jobClasstNames.size() is :" + jobClassNames.size());
        Log.getLogWriter().info("jobClasstNames are :" + jobClassNames.toString());
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                Log.getLogWriter().info("SS - Inside loop for getting jobClasstNames and executing them");
                String userJob = (String) jobClassNames.elementAt(i);
                Log.getLogWriter().info("SS - Snappy job class name is :: " + userJob);
                Log.getLogWriter().info("Executing Snappy Job script... ");
                pb = new ProcessBuilder(snappyJobScript, "submit", "--lead", leadHost + ":8090", "--app-name", "myapp", "--class", userJob, "--app-jar", snappyTest.getUserAppJarLocation(userAppJar), "--stream");
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + logFileName;
                logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
            }
//            sleep(waitTimeBeforeJobStatus);
            snappyTest.getSnappyJobsStatus(snappyJobScript, logFile);
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected void executeSnappyJob(Vector jobClassNames, String logFileName) {
        String snappyJobScript = snappyTest.getSnappyPath() + sep + "bin" + sep + "snappy-job.sh";
        ProcessBuilder pb = null;
        File logFile = null;
        Log.getLogWriter().info("jobClasstNames.size() is :" + jobClassNames.size());
        Log.getLogWriter().info("jobClasstNames are :" + jobClassNames.toString());
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                Log.getLogWriter().info("SS - Inside loop for getting jobClasstNames and executing them");
                String userJob = (String) jobClassNames.elementAt(i);
                Log.getLogWriter().info("SS - Snappy job class name is :: " + userJob);
                Log.getLogWriter().info("Executing Snappy Job script... ");
                pb = new ProcessBuilder(snappyJobScript, "submit", "--lead", leadHost + ":8090", "--app-name", "myapp", "--class", userJob, "--app-jar", snappyTest.getUserAppJarLocation(userAppJar));
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + logFileName;
                logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
            }
//            sleep(waitTimeBeforeJobStatus);
            snappyTest.getSnappyJobsStatus(snappyJobScript, logFile);
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected void simulateStream() {
        String fileName = "simulateFileStreamResult.log";
        ProcessBuilder pb = null;
        File logFile = null;
        String streamScriptName = snappyTest.getUserScriptLocation(simulateStreamScriptName);
        String command = streamScriptName;
        Log.getLogWriter().info("streamScriptName is :" + streamScriptName.toString());
        try {
            Log.getLogWriter().info("Executing simulateStream script... ");
            pb = new ProcessBuilder(command, simulateStreamScriptDestinationFolder, snappyTest.getSnappyPath());
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + fileName;
            logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
            Log.getLogWriter().info("Done executing simulateStream script... ");
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected void getSnappyJobsStatus(String snappyJobScript, File logFile) {
        try {
            String line = null;
            Set<String> jobIds = new LinkedHashSet<String>();
            FileReader freader = new FileReader(logFile);
            BufferedReader inputFile = new BufferedReader(freader);
            while ((line = inputFile.readLine()) != null) {
                if (line.contains("jobId")) {
                    String jobID = line.split(":")[1].trim();
                    jobID = jobID.substring(1, jobID.length() - 2);
                    Log.getLogWriter().info("jobID is ::" + jobID);
                    jobIds.add(jobID);
                    Log.getLogWriter().info("jobIds are" + jobIds.toString());
                }
            }
            inputFile.close();
            for (String str : jobIds) {
                ProcessBuilder pb = new ProcessBuilder(snappyJobScript, "status", "--lead", leadHost + ":8090", "--job-id", str);
                snappyTest.executeProcess(pb, logFile);
            }
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected File writeDataToSQLFile(Vector scriptNames, String filename) {
        try {
            Log.getLogWriter().info("scriptSize is :" + scriptNames.size());
            Log.getLogWriter().info("scriptNames are :" + scriptNames.toString());
            File dir = new File(".");
            String dest = dir.getCanonicalPath() + File.separator + filename;
            File sqlFile = new File(dest);
            String connectClient = "connect client 'localhost:1527';";
            FileWriter fstream = new FileWriter(dest, true);
            BufferedWriter bw = new BufferedWriter(fstream);
            bw.write(connectClient);
            Log.getLogWriter().info("SS - client command added successfully");
            bw.newLine();
            for (int i = 0; i < scriptNames.size(); i++) {
                Log.getLogWriter().info("SS - Inside loop in stdin for getting scriptnames and executing them");
                String userScript = (String) scriptNames.elementAt(i);
                Log.getLogWriter().info("SS - user script is :: " + userScript);
                String fileInput = snappyTest.getUserScriptLocation(userScript);
                File fin = new File(fileInput);
                FileInputStream fis = new FileInputStream(fin);
                BufferedReader in = new BufferedReader(new InputStreamReader(fis));
                String aLine = null;
                while ((aLine = in.readLine()) != null) {
                    //Process each line and add output to Dest.txt file
                    bw.write(aLine);
                    bw.newLine();
                }
                // do not forget to close the buffer reader
                in.close();
            }
            bw.write("exit;");
            // close buffer writer
            bw.close();
            return sqlFile;

        } catch (IOException e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }


    /*
   * Returns the log file name.  Autogenerates the directory name at runtime
   * using the same path as the master.  The directory is created if needed.
   *
   * @throws HydraRuntimeException if the directory cannot be created.
   */
    private synchronized String getLogDir() {
        if (this.logFile == null) {
            HostDescription hd = TestConfig.getInstance().getMasterDescription()
                    .getVmDescription().getHostDescription();
            String name = TestConfig.tab().stringAt(ClientPrms.gemfireNames, "gemfire1");
            String dirname = hd.getUserDir() + File.separator
                    + "vm_" + RemoteTestModule.getMyVmid()
                    + "_" + RemoteTestModule.getMyClientName()
                    + "_" + HostHelper.getLocalHost()
                    + "_" + RemoteTestModule.getMyPid();
            log().info("SS - dirname is ::" + dirname);
            this.localHost = HostHelper.getLocalHost();
            File dir = new File(dirname);
            String fullname = dir.getAbsolutePath();
            log().info("SS - dirPath is ::" + fullname);
            try {
                FileUtil.mkdir(dir);
                log().info("SS - created the log directory" + dir.getPath());
                try {
                    RemoteTestModule.Master.recordDir(hd,
                            name, fullname);
                } catch (RemoteException e) {
                    String s = "Unable to access master to record directory: " + dir;
                    throw new HydraRuntimeException(s, e);
                }
            } catch (VirtualMachineError e) {
                SystemFailure.initiateFailure(e);
                throw e;
            } catch (Error e) {
                String s = "Unable to create directory: " + dir;
                throw new HydraRuntimeException(s);
            }
            this.logFile = dirname; //+ File.separator + "system.log";
            log().info("SS - logFile name is ::" + this.logFile);
        }
        return this.logFile;
    }


    protected synchronized void generateConfig(String fileName) {
        String confPath = snappyTest.getSnappyPath() + sep + "conf";
        try {
            String path = confPath + sep + fileName;
            log().info("File Path is ::" + path);
            File file = new File(path);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            } else if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getStackTrace());
        }
        Log.getLogWriter().info("Done with generateConfig...");

    }

    public static void HydraTask_deleteSnappyConfig() throws IOException {
        String confPath = snappyTest.getSnappyPath() + sep + "conf";
        String locatorConf = confPath + sep + "locators";
        String serverConf = confPath + sep + "servers";
        String leadConf = confPath + sep + "leads";
        Files.delete(Paths.get(locatorConf));
        Log.getLogWriter().info("locators file deleted");
        Files.delete(Paths.get(serverConf));
        Log.getLogWriter().info("Servers file deleted");
        Files.delete(Paths.get(leadConf));
        Log.getLogWriter().info("leads file deleted");
    }

    protected int getMyTid() {
        int myTid = RemoteTestModule.getCurrentThread().getThreadId();
        return myTid;
    }

    public static void HydraTask_cycleStoreVms() {
        if (cycleVms) snappyTest.cycleStoreVms();
    }

    protected void cycleStoreVms() {
        if (!cycleVms) {
            Log.getLogWriter().warning("cycleVms sets to false, no node will be brought down in the test run");
            return;
        }
        int numToKill = TestConfig.tab().intAt(StopStartPrms.numVMsToStop, 1);
        List<ClientVmInfo> vms = null;
        if (SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.stopStartVms) == 1) {
            Object vmCycled = SnappyBB.getBB().getSharedMap().get("vmCycled");
            if (vmCycled == null) {

                while (true) {
                    try {
                        vms = stopStartVMs(numToKill);
                        break;
                    } catch (TestException te) {
                    }
                }
            } //first time
            else {
                //relaxing a little for HA tests
                //using the BB to track when to kill the next set of vms
                Long lastCycledTimeFromBB = (Long) SnappyBB.getBB().getSharedMap().get(LASTCYCLEDTIME);
                if (lastCycledTimeFromBB == null) {
                    int sleepMS = 20000;
                    Log.getLogWriter().info("allow  " + sleepMS / 1000 + " seconds before killing others");
                    MasterController.sleepForMs(sleepMS); //no vms has been cycled before
                } else if (lastCycledTimeFromBB > lastCycledTime) {
                    lastCycledTime = lastCycledTimeFromBB;
                    log().info("update last cycled vms is set to " + lastCycledTime);
                }

                if (lastCycledTime != 0) {
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastCycledTime < waitTimeBeforeNextCycleVM * THOUSAND) {
                        SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartVms);
                        return;
                    } else {
                        log().info("cycle vms starts at: " + currentTime);
                    }
                }

                vms = stopStartVMs(numToKill);
            }
            if (vms == null || vms.size() == 0) {
                Log.getLogWriter().info("no vm being chosen to be stopped"); //choose the same vm has dbsynchronizer
                SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartVms);
                return;
            }
            long currentTime = System.currentTimeMillis();
            log().info("cycle vms finishes at: " + currentTime);
            SnappyBB.getBB().getSharedMap().put(LASTCYCLEDTIME, currentTime);
            SnappyBB.getBB().getSharedMap().put("vmCycled", "true");
            SnappyBB.getBB().getSharedCounters().zero(SnappyBB.stopStartVms);
        }
    }

    protected List<ClientVmInfo> stopStartVMs(int numToKill) {
        return stopStartVMs(numToKill, cycleVMTarget);
    }

    @SuppressWarnings("unchecked")
    protected List<ClientVmInfo> stopStartVMs(int numToKill, String target) {
        Object[] tmpArr = StopStartVMs.getOtherVMs(numToKill, target);
        // get the VMs to stop; vmList and stopModeList are parallel lists

        Object vm1 = SnappyBB.getBB().getSharedMap().get("storeVMTarget1");
        Object vm2 = SnappyBB.getBB().getSharedMap().get("storeVMTarget2");
        List<ClientVmInfo> vmList;
        List<String> stopModeList;

        if (vm1 == null && vm2 == null) {
            vmList = (List<ClientVmInfo>) (tmpArr[0]);
            stopModeList = (List<String>) (tmpArr[1]);
            for (ClientVmInfo client : vmList) {
                PRObserver.initialize(client.getVmid());
            } //clear bb info for the vms to be stopped/started
        } else {
            vmList = (List<ClientVmInfo>) (tmpArr[0]);
            stopModeList = (List<String>) (tmpArr[1]);
            for (int i = 0; i < vmList.size(); i++) {
                if (vmList.get(i).getVmid().intValue() == ((ClientVmInfo) vm1).getVmid().intValue()
                        || vmList.get(i).getVmid().intValue() == ((ClientVmInfo) vm2).getVmid().intValue()) {
                    Log.getLogWriter().info("remove the vm " + vmList.get(i).getVmid() + " from the stop list");
                    vmList.remove(i);
                } else PRObserver.initialize(vmList.get(i).getVmid());
            }//clear bb info for the vms to be stopped/started
        }
        if (vmList.size() != 0) StopStartVMs.stopStartVMs(vmList, stopModeList);
        return vmList;
    }


    /**
     * Create and start snappy locator.
     */
    public static synchronized void HydraTask_createAndStartSnappyLocator() {
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.locatorsStarted);
            if (num == 1) {
                Log.getLogWriter().info("Executing locator start script...");
                ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "start");
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "snappyLocatorSystem.log";
                File logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                snappyTest.recordSnappyProcessIDinNukeRun("LocatorLauncher");
            }
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    /**
     * Create and start snappy server.
     */
    public static synchronized void HydraTask_createAndStartSnappyServers() {
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.serversStarted);
            if (num == 1) {
                Log.getLogWriter().info("Executing server start script... ");
                ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "start");
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
                File logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                snappyTest.recordSnappyProcessIDinNukeRun("ServerLauncher");
            }
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    /**
     * Creates and start snappy lead.
     */
    public static synchronized void HydraTask_createAndStartSnappyLeader() {
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.leadsStarted);
            if (num == 1) {
                Log.getLogWriter().info("Executing lead start script...");
                ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"), "start");
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "snappyLeaderSystem.log";
                File logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                snappyTest.recordSnappyProcessIDinNukeRun("LeaderLauncher");
            }
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    /**
     * Stops snappy lead.
     */
    public static synchronized void HydraTask_stopSnappyLeader() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"), "stop").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    /**
     * Stops snappy server/servers.
     */
    public static synchronized void HydraTask_stopSnappyServers() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "stop").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    /**
     * Stops a snappy locator.
     */
    public static synchronized void HydraTask_stopSnappyLocator() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "stop").start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    public static synchronized void HydraTask_stopSnappyCluster() {
        try {
            Process pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-stop-all.sh")).start();
            int rc = pb.waitFor();
            System.out.printf("Script executed with exit code %d\n", rc);
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected LogWriter log() {
        return Log.getLogWriter();
    }

}
