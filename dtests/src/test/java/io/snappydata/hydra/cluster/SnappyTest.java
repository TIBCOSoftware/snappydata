package io.snappydata.hydra.cluster;


import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;

import hydra.*;
import org.apache.spark.sql.SnappyContext;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import util.TestException;

import static java.lang.Thread.sleep;

public class SnappyTest implements Serializable {

    private static transient SnappyContext snc = SnappyContext.getOrCreate(SnappyContext.globalSparkContext());
    protected static SnappyTest snappyTest;
    private static HostDescription hd = TestConfig.getInstance().getMasterDescription()
            .getVmDescription().getHostDescription();
    private static char sep = hd.getFileSep();
    private static String gemfireHome = hd.getGemFireHome() + sep;
    private static String productDir = gemfireHome + ".." + sep + "snappy" + sep;
    private static String productConfDirPath = productDir + "conf" + sep;
    private static String productLibsDir = productDir + "lib" + sep;
    private static String productSbinDir = productDir + "sbin" + sep;
    private static String productBinDir = productDir + "bin" + sep;
    private static String SnappyShellPath = productBinDir + "snappy-shell";
    private static String dtests = gemfireHome + ".." + sep + ".." + sep + ".." + sep + "dtests" + sep;
    private static String dtestsLibsDir = dtests + "build-artifacts" + sep + "scala-2.10" + sep + "libs" + sep;
    private static String dtestsResourceLocation = dtests + "src" + sep + "resources" + sep;
    private static String dtestsScriptLocation = dtestsResourceLocation + "scripts" + sep;
    private static String dtestsDataLocation = dtestsResourceLocation + "data" + sep;
    private static String quickstartScriptLocation = productDir + "quickstart" + sep + "scripts" + sep;
    private static String quickstartDataLocation = productDir + "quickstart" + sep + "data" + sep;
    private static String logFile = null;

    private static Set<Integer> pids = new LinkedHashSet<Integer>();
    private static String locatorsFilePath = null;
    private static String serversFilePath = null;
    private static String leadsFilePath = null;
    private static String userAppJar = null;
    private static String simulateStreamScriptName = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptName, "simulateFileStream");
    private static String simulateStreamScriptDestinationFolder = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptDestinationFolder, dtestsResourceLocation);
    private static String leadHost = null;
    public static Long waitTimeBeforeJobStatusInTask = TestConfig.tab().longAt(SnappyPrms.jobExecutionTimeInMillisForTask, 6000);
    public static Long waitTimeBeforeStreamingJobStatusInTask = TestConfig.tab().longAt(SnappyPrms.streamingJobExecutionTimeInMillisForTask, 6000);
    public static Long waitTimeBeforeJobStatusInCloseTask = TestConfig.tab().longAt(SnappyPrms.jobExecutionTimeInMillisForCloseTask, 6000);
    private static Boolean logDirExists = false;
    private Connection connection = null;
    private static HydraThreadLocal localconnection = new HydraThreadLocal();

    public static <A, B> Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>>conforms());
    }

    public static void HydraTask_stopSnappy() {
        SnappyContext.stop(true);
        Log.getLogWriter().info("SnappyContext stopped successfully");
    }

    public static void HydraTask_initializeSnappyTest() {
        if (snappyTest == null) {
            snappyTest = new SnappyTest();
            snappyTest.getClientHostDescription();
            snappyTest.generateConfig("locators");
            snappyTest.generateConfig("servers");
            snappyTest.generateConfig("leads");
        }
    }

    protected void getClientHostDescription() {
        hd = TestConfig.getInstance()
                .getClientDescription(RemoteTestModule.getMyClientName())
                .getVmDescription().getHostDescription();
    }

    protected String getUserAppJarLocation(String userAppJar) {
        String jarPath = null;
        jarPath = dtestsLibsDir + userAppJar;
        if (!new File(jarPath).exists()) {
            String s = "User App jar doesn't exists at expected location: " + dtestsLibsDir;
            throw new TestException(s);
        }
        return jarPath;
    }

    protected String getDataLocation(String paramName) {
        String scriptPath = null;
        if (new File(paramName).exists()) {
            return paramName;
        } else {
            scriptPath = quickstartDataLocation + paramName;
            if (new File(scriptPath).exists()) return scriptPath;
            else scriptPath = dtestsDataLocation + paramName;
            if (new File(scriptPath).exists()) return scriptPath;
            else {
                String s = "Data doesn't exists at any expected location.";
                throw new TestException(s);
            }
        }
    }

    protected String getScriptLocation(String scriptName) {
        String scriptPath = null;
        scriptPath = productSbinDir + scriptName;
        if (!new File(scriptPath).exists()) {
            scriptPath = productBinDir + scriptName;
            if (new File(scriptPath).exists()) return scriptPath;
            else
                scriptPath = dtestsScriptLocation + scriptName;
            if (new File(scriptPath).exists()) return scriptPath;
            else
                scriptPath = quickstartScriptLocation + scriptName;
            if (new File(scriptPath).exists()) return scriptPath;
            else {
                String s = "Unable to find the script at any expected location.";
                throw new TestException(s);
            }
        }
        return scriptPath;
    }


    public static synchronized void HydraTask_generateSnappyConfig() {
        snappyTest.generateSnappyConfig();
    }

    protected void generateSnappyConfig() {
        if (logDirExists) return;
        else {
            String addr = HostHelper.getHostAddress();
            int port = PortHelper.getRandomPort();
            String endpoint = addr + ":" + port;
            String clientPort = " -client-port=";
            String locators = "-locators=";
            String locatorHost = null;
            String dirPath = snappyTest.getLogDir();
            if (dirPath.contains("locator")) {
                String locatorLogDir = HostHelper.getLocalHost() + " -dir=" + dirPath + clientPort + port;
                if (locatorLogDir == null) {
                    String s = "Unable to find " + RemoteTestModule.getMyClientName() + " log directory path for writing to the locators file under conf directory";
                    throw new TestException(s);
                }
                SnappyBB.getBB().getSharedMap().put("locatorLogDir" + "_" + snappyTest.getMyTid(), locatorLogDir);
                String portString = port + "";
                SnappyBB.getBB().getSharedMap().put("locatorHost", HostHelper.getLocalHost());
                SnappyBB.getBB().getSharedMap().put("locatorPort", portString);
                Log.getLogWriter().info("Generated locator endpoint: " + endpoint);
                SnappyNetworkServerBB.getBB().getSharedMap().put("locator" + "_" + RemoteTestModule.getMyVmid(), endpoint);
            } else if (dirPath.contains("Store") || dirPath.contains("server")) {
                locatorHost = (String) SnappyBB.getBB().getSharedMap().get("locatorHost");
                //String serverLogDir = HostHelper.getLocalHost() + " " + locators + locatorHost + ":" + 10334 + " -dir=" + dirPath + clientPort + port;

                String serverLogDir = HostHelper.getLocalHost() + " " + locators + locatorHost + ":" + 10334 + " -dir=" + dirPath + clientPort + port + " -J-Xmx" + SnappyPrms.getServerMemory() + " -conserve-sockets=" + SnappyPrms.getConserveSockets();

                if (serverLogDir == null) {
                    String s = "Unable to find " + RemoteTestModule.getMyClientName() + " log directory path for writing to the servers file under conf directory";
                    throw new TestException(s);
                }
                SnappyBB.getBB().getSharedMap().put("serverLogDir" + "_" + snappyTest.getMyTid(), serverLogDir);
                Log.getLogWriter().info("Generated peer server endpoint: " + endpoint);
                SnappyNetworkServerBB.getBB().getSharedMap().put("server" + "_" + RemoteTestModule.getMyVmid(), endpoint);
            } else if (dirPath.contains("lead")) {
                locatorHost = (String) SnappyBB.getBB().getSharedMap().get("locatorHost");
                //String leadLogDir = HostHelper.getLocalHost() + " " + locators + locatorHost + ":" + 10334 + " -dir=" + dirPath + clientPort + port;

                String leadLogDir = HostHelper.getLocalHost() + " " + locators + locatorHost + ":" + 10334 + " -spark.executor.cores="+ SnappyPrms.getExecutorCores() + " -spark.driver.maxResultSize="+SnappyPrms.getDriverMaxResultSize()+" -dir=" + dirPath + clientPort + port + " -J-Xmx"+ SnappyPrms.getLeadMemory()
                    + " -spark.sql.autoBroadcastJoinThreshold=" + SnappyPrms.getSparkSqlBroadcastJoinThreshold() + " -spark.scheduler.mode=" + SnappyPrms.getSparkSchedulerMode() +" -spark.sql.inMemoryColumnarStorage.compressed=" + SnappyPrms.getCompressedInMemoryColumnarStorage() + " -conserve-sockets=" + SnappyPrms.getConserveSockets();

                Log.getLogWriter().info("KBKBKB : " + leadLogDir);
                if (leadLogDir == null) {
                    String s = "Unable to find " + RemoteTestModule.getMyClientName() + " log directory path for writing to the leads file under conf directory";
                    throw new TestException(s);
                }
                SnappyBB.getBB().getSharedMap().put("leadLogDir" + "_" + snappyTest.getMyTid(), leadLogDir);
                if (leadHost == null) {
                    leadHost = HostHelper.getLocalHost();
                }
                Log.getLogWriter().info("Lead host is: " + leadHost);
            }
            logDirExists = true;
        }
    }

    protected static Set<String> getFileContents(String userKey, Set<String> fileContents) {
        Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
        for (String key : keys) {
            if (key.startsWith(userKey)) {
                Log.getLogWriter().info("Key Found..." + key);
                String value = (String) SnappyBB.getBB().getSharedMap().get(key);
                fileContents.add(value);
            }
        }
        return fileContents;
    }

    public static void HydraTask_writeConfigDataToFiles() {
        snappyTest.writeConfigDataToFiles();
    }

    protected void writeConfigDataToFiles() {
        locatorsFilePath = productConfDirPath + "locators";
        serversFilePath = productConfDirPath + "servers";
        leadsFilePath = productConfDirPath + "leads";
        File locatorsFile = new File(locatorsFilePath);
        File serversFile = new File(serversFilePath);
        File leadsFile = new File(leadsFilePath);
        Set<String> locatorsFileContent = new LinkedHashSet<String>();
        Set<String> serversFileContent = new LinkedHashSet<String>();
        Set<String> leadsFileContent = new LinkedHashSet<String>();

        locatorsFileContent = snappyTest.getFileContents("locatorLogDir", locatorsFileContent);
        serversFileContent = snappyTest.getFileContents("serverLogDir", serversFileContent);
        leadsFileContent = snappyTest.getFileContents("leadLogDir", leadsFileContent);
        if (locatorsFileContent.size() == 0) {
            String s = "No data found for writing to locators file under conf directory";
            throw new TestException(s);
        }
        for (String s : locatorsFileContent) {
            snappyTest.writeToFile(s, locatorsFile);
        }
        if (serversFileContent.size() == 0) {
            String s = "No data found for writing to servers file under conf directory";
            throw new TestException(s);
        }
        for (String s : serversFileContent) {
            snappyTest.writeToFile(s, serversFile);
        }
        if (leadsFileContent.size() == 0) {
            String s = "No data found for writing to leads file under conf directory";
            throw new TestException(s);
        }
        for (String s : leadsFileContent) {
            snappyTest.writeToFile(s, leadsFile);
        }
    }

    /**
     * Returns all network locator endpoints from the {@link
     * SnappyNetworkServerBB} map, a possibly empty list.  This includes all
     * network servers that have ever started, regardless of their distributed
     * system or current active status.
     */
    public static List getNetworkLocatorEndpoints() {
        return getEndpoints("locator");
    }

    /**
     * Returns all network server endpoints from the {@link
     * SnappyNetworkServerBB} map, a possibly empty list.  This includes all
     * network servers that have ever started, regardless of their distributed
     * system or current active status.
     */
    public static List getNetworkServerEndpoints() {
        return getEndpoints("server");
    }

    protected int getClientPort() {
        try {
            List<String> endpoints = getNetworkLocatorEndpoints();
            if (endpoints.size() == 0) {
                String s = "No network server endpoints found";
                throw new TestException(s);
            }
            String endpoint = endpoints.get(0);
            String port = endpoint.substring(endpoint.indexOf(":") + 1);
            Log.getLogWriter().info("port string is:" + port);
            int clientPort = Integer.parseInt(port);
            Log.getLogWriter().info("Client Port is :" + clientPort);
            return clientPort;
        } catch (Exception e) {
            String s = "No client port found";
            throw new TestException(s);
        }
    }


    /**
     * Returns all endpoints of the given type.
     */
    private static synchronized List<String> getEndpoints(String type) {
        List<String> endpoints = new ArrayList();
        Set<String> keys = SnappyNetworkServerBB.getBB().getSharedMap().getMap().keySet();
        Log.getLogWriter().info("Complete endpoint list contains: " + keys);
        for (String key : keys) {
            if (key.startsWith(type.toString())) {
                String endpoint = (String) SnappyNetworkServerBB.getBB().getSharedMap().getMap().get(key);
                Log.getLogWriter().info("endpoint Found...." + endpoint);
                endpoints.add(endpoint);
            }
        }
        Log.getLogWriter().info("Returning endpoint list: " + endpoints);
        return endpoints;
    }


    /**
     * Returns PIDs for all the processes started in the test, e.g. locator, server, lead .
     */
    private static synchronized List<String> getPidList() {
        List<String> pidList = new ArrayList();
        Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
        for (String key : keys) {
            if (key.startsWith("pid")) {
                String pid = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
                pidList.add(pid);
            }
        }
        Log.getLogWriter().info("Returning pid list: " + pidList);
        return pidList;
    }

    //TODO: SWATI can you please incorporate this hydrathreadlocal changes for your tests too. Here coonection is made threadlocal
    protected void initHydraThreadLocals() {
        this.connection = getConnection();
    }

    protected Connection getConnection() {
        Connection connection = (Connection)localconnection.get();
        return connection;
    }

    protected void setConnection(Connection connection) {
        localconnection.set(connection);
    }

    protected void updateHydraThreadLocals() {
        setConnection(this.connection);
    }

    public static void HydraTask_getClientConnection_Snappy() throws SQLException {
        SnappyTest st = new SnappyTest();
        st.connectThinClient();
        st.updateHydraThreadLocals();
    }

    private void connectThinClient() throws SQLException {
        connection = getLocatorConnection();
    }

    public static Connection getClientConnection(){
        SnappyTest st = new SnappyTest();
        st.initHydraThreadLocals();
        return st.getConnection();
    }

    public static void HydraTask_getClientConnection() throws SQLException {
        getLocatorConnection();
    }


    /**
     * Gets Client connection.
     */
    public static Connection getLocatorConnection() throws SQLException {
        List<String> endpoints = getNetworkLocatorEndpoints();
        if (endpoints.size() == 0) {
            String s = "No network locator endpoints found";
            throw new TestException(s);
        }
        String url = "jdbc:snappydata://" + endpoints.get(0);
        Log.getLogWriter().info("url is " + url);
        return getConnection(url, "com.pivotal.gemfirexd.jdbc.ClientDriver");
    }

    private static Connection getConnection(String protocol, String driver) throws SQLException {
        Log.getLogWriter().info("Creating connection using " + driver + " with " + protocol);
        loadDriver(driver);
        Connection conn = DriverManager.getConnection(protocol);
        return conn;
    }

    /**
     * The JDBC driver is loaded by loading its class.  If you are using JDBC 4.0
     * (Java SE 6) or newer, JDBC drivers may be automatically loaded, making
     * this code optional.
     * <p/>
     * In an embedded environment, any static Derby system properties
     * must be set before loading the driver to take effect.
     */
    public static void loadDriver(String driver) {
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            String s = "Problem loading JDBC driver: " + driver;
            throw new TestException(s, e);
        } catch (InstantiationException e) {
            String s = "Problem loading JDBC driver: " + driver;
            throw new TestException(s, e);
        } catch (IllegalAccessException e) {
            String s = "Problem loading JDBC driver: " + driver;
            throw new TestException(s, e);
        }
    }

    public static void runQuery() throws SQLException {
        Connection conn = getLocatorConnection();
        String query1 = "SELECT count(*) FROM airline";
        ResultSet rs = conn.createStatement().executeQuery(query1);
        while (rs.next()) {
            Log.getLogWriter().info("Qyery executed successfully and query result is ::" + rs.getLong(1));
        }
    }

    protected void writeToFile(String logDir, File file) {
        try {
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(logDir);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            throw new TestException("Error occurred while writing to a file: " + file + e.getMessage());
        }
    }

    /**
     * Executes user scripts for InitTask.
     */
    public static void HydraTask_executeSQLScriptsInInitTask() {
        Vector scriptNames, paramList = null;
        File log = null, logFile = null;
        scriptNames = SnappyPrms.getSQLScriptNamesForInitTask();
        if (scriptNames == null) {
            String s = "No Script names provided for executing in INITTASK";
            throw new TestException(s);
        }
        paramList = SnappyPrms.getSQLScriptParamsForInitTask();
        if (paramList == null) {
            String s = "Required Parameter(sqlScriptParamsForInitTask) not found for executing scripts in INITTASK.";
            throw new TestException(s);
        }
        try {
            for (int i = 0; i < scriptNames.size(); i++) {
                String userScript = (String) scriptNames.elementAt(i);
                String param = (String) paramList.elementAt(i);
                String path = snappyTest.getDataLocation(param);
                String filePath = snappyTest.getScriptLocation(userScript);
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "sqlScriptsInitTaskResult.log";
                logFile = new File(dest);
                int clientPort = snappyTest.getClientPort();
                ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" + filePath, "-param:path=" + path, "-client-port=" + clientPort);
                snappyTest.executeProcess(pb, logFile);
            }
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
        }
    }

    protected void executeProcess(ProcessBuilder pb, File logFile) {
        Process p = null;
        try {
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
            p = pb.start();
            assert pb.redirectInput() == ProcessBuilder.Redirect.PIPE;
            assert pb.redirectOutput().file() == logFile;
            assert p.getInputStream().read() == -1;
            int rc = p.waitFor();
        } catch (IOException e) {
            throw new TestException("Exception occurred while starting the process:" + pb + "\nError Message:" + e.getMessage());
        } catch (InterruptedException e) {
            throw new TestException("Exception occurred while waiting for the process execution:" + p + "\nError Message:" + e.getMessage());
        }
    }

    protected void recordSnappyProcessIDinNukeRun(String pName) {
        Process pr = null;
        try {
            String command = "ps ax | grep " + pName + " | grep -v grep | awk '{print $1}'";
            hd = TestConfig.getInstance().getMasterDescription()
                    .getVmDescription().getHostDescription();
            ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "PIDs.log";
            File logFile = new File(dest);
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
            pr = pb.start();
            pr.waitFor();
            FileInputStream fis = new FileInputStream(logFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String str = null;
            while ((str = br.readLine()) != null) {
                int pid = Integer.parseInt(str);
                try {
                    if (pids.contains(pid)) {
                        Log.getLogWriter().info("Pid is already recorded with Master" + pid);
                    } else {
                        pids.add(pid);
                        RemoteTestModule.Master.recordPID(hd, pid);
                        SnappyBB.getBB().getSharedMap().put("pid" + "_" + str, str);
                    }
                } catch (RemoteException e) {
                    String s = "Unable to access master to record PID: " + pid;
                    throw new HydraRuntimeException(s, e);
                }
                Log.getLogWriter().info("pid value successfully recorded with Master");
            }
            br.close();
        } catch (IOException e) {
            String s = "Problem while starting the process : " + pr;
            throw new TestException(s, e);
        } catch (InterruptedException e) {
            String s = "Exception occurred while waiting for the process execution : " + pr;
            throw new TestException(s, e);
        }
    }

    /**
     * Task(ENDTASK) for cleaning up snappy processes, because they are not stopped by Hydra in case of Test failure.
     */
    public static void HydraTask_cleanUpSnappyProcessesOnFailure() {
        Process pr = null;
        ProcessBuilder pb = null;
        File logFile = null, log = null, nukeRunOutput = null;
        try {
            List<String> pidList = new ArrayList();
            HostDescription hd = TestConfig.getInstance().getMasterDescription()
                    .getVmDescription().getHostDescription();
            pidList = snappyTest.getPidList();
            log = new File(".");
            String nukerun = log.getCanonicalPath() + File.separator + "snappyNukeRun.sh";
            logFile = new File(nukerun);
            String nukeRunOutputString = log.getCanonicalPath() + File.separator + "nukeRunOutput.log";
            nukeRunOutput = new File(nukeRunOutputString);
            FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            for (String pidString : pidList) {
                int pid = Integer.parseInt(pidString);
                bw.write("/bin/kill -KILL " + pid);
                bw.newLine();
                try {
                    RemoteTestModule.Master.removePID(hd, pid);
                } catch (RemoteException e) {
                    String s = "Failed to remove PID from nukerun script: " + pid;
                    throw new HydraRuntimeException(s, e);
                }
            }
            bw.close();
            fw.close();
            Log.getLogWriter().info("Done writing to snappyNukeRun file... ");
            logFile.setExecutable(true);
            pb = new ProcessBuilder(nukerun);
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(nukeRunOutput));
            pr = pb.start();
            pr.waitFor();
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
        } catch (InterruptedException e) {
            String s = "Exception occurred while waiting for the process execution : " + pr;
            throw new TestException(s, e);
        }
    }

    /**
     * Executes user scripts for InitTask.
     */
    public static void HydraTask_executeSQLScriptsInTask() {
        Vector scriptNames = null;
        File log = null;
        scriptNames = SnappyPrms.getSQLScriptNamesForTask();
        if (scriptNames == null) {
            String s = "No Script names provided for executing in TASK";
            throw new TestException(s);
        }
        try {
            for (int i = 0; i < scriptNames.size(); i++) {
                String userScript = (String) scriptNames.elementAt(i);
                String filePath = snappyTest.getScriptLocation(userScript);
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "sqlScriptsTaskResult.log";
                File logFile = new File(dest);
                int clientPort = snappyTest.getClientPort();
                ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" + filePath, "-client-port=" + clientPort);
                snappyTest.executeProcess(pb, logFile);
            }
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
        }
    }

    /**
     * Executes snappy Jobs in Close Task.
     */
    public static void HydraTask_executeSnappyJobInCloseTask() {
        int currentThread = snappyTest.getMyTid();
        String logFile = "snappyJobCloseTaskResult_thread_" + currentThread + ".log";
        SnappyBB.getBB().getSharedMap().put("logFilesForCloseTask" + currentThread, logFile);
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNamesForCloseTask(), logFile);
        try {
            sleep(waitTimeBeforeJobStatusInCloseTask);
        } catch (InterruptedException e) {
            throw new TestException("Exception occurred while waiting for the snappy job process execution." + "\nError Message:" + e.getMessage());
        }
    }

    /**
     * Executes snappy Jobs in Task.
     */
    public static void HydraTask_executeSnappyJobInTask() {
        int currentThread = snappyTest.getMyTid();
        String logFile = "snappyJobTaskResult_thread_" + currentThread + ".log";
        SnappyBB.getBB().getSharedMap().put("logFilesForTask" + currentThread, logFile);
        snappyTest.executeSnappyJob(SnappyPrms.getSnappyJobClassNamesForTask(), logFile);
        try {
            sleep(waitTimeBeforeJobStatusInTask);
        } catch (InterruptedException e) {
            throw new TestException("Exception occurred while waiting for the snappy job process execution." + "\nError Message:" + e.getMessage());
        }
    }

    public static void HydraTask_executeSnappyStreamingJob_benchmarking() {
        snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNamesForTask(),
            "snappyStreamingJobTaskResult" + System.currentTimeMillis() + ".log");
    }


    /**
     * Executes snappy Streaming Jobs in Task.
     */

    public static void HydraTask_executeSnappyStreamingJob() {
        Runnable fileStreaming = new Runnable() {
            public void run() {
                snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNamesForTask(), "snappyJobTaskResult.log");
            }
        };

        Runnable simulateFileStream = new Runnable() {
            public void run() {
                snappyTest.simulateStream();
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(fileStreaming);
        es.submit(simulateFileStream);
        try {
            Log.getLogWriter().info("Sleeping for " + waitTimeBeforeStreamingJobStatusInTask + "millis before executor service shut down");
            Thread.sleep(waitTimeBeforeStreamingJobStatusInTask);
            es.shutdown();
            es.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new TestException("Exception occurred while waiting for the snappy streaming job process execution." + "\nError Message:" + e.getMessage());
        }
    }

    protected void executeSnappyStreamingJob(Vector jobClassNames, String logFileName) {
        String snappyJobScript = getScriptLocation("snappy-job.sh");
        ProcessBuilder pb = null;
        File log = null;
        File logFile = null;
        userAppJar = TestConfig.tab().stringAt(SnappyPrms.userAppJar);
        snappyTest.verifyDataForJobExecution(jobClassNames, userAppJar);
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                String userJob = (String) jobClassNames.elementAt(i);
                pb = new ProcessBuilder(snappyJobScript, "submit", "--lead", leadHost + ":8090", "--app-name", "myapp", "--class", userJob, "--app-jar", snappyTest.getUserAppJarLocation(userAppJar), "--stream");
                java.util.Map<String, String> env = pb.environment();
                env.put("APP_PROPS",  SnappyPrms.getCommaSepAPPProps() + ",shufflePartitions="+SnappyPrms.getShufflePartitions());
                log = new File(".");
                //TODO : For Swati: For Ch benchmark testing we are reading Snappyprms-appPropsForJobServer. we need to decode them in nice way. crude way is given above.
//                String APP_PROPS = "\"dataDirName=" + simulateStreamScriptDestinationFolder + "\"";
//                String contextName = "snappyStreamingContext" + System.currentTimeMillis();
//                String contextFactory = "org.apache.spark.sql.streaming.SnappyStreamingContextFactory";
//                String curlCommand1 = "curl --data-binary @" + snappyTest.getUserAppJarLocation(userAppJar) + " " + leadHost + ":8090/jars/myapp";
//                String curlCommand2 = "curl -d  \"\"" + " '" + leadHost + ":8090/" + "contexts/" + contextName + "?context-factory=" + contextFactory + "'";
//                String curlCommand3 = "curl -d " + APP_PROPS + " '" + leadHost + ":8090/jobs?appName=myapp&classPath=" + userJob + "&context=" + contextName + "'";
//                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand1);
//                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + logFileName;
                logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
//                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand2);
//                snappyTest.executeProcess(pb, logFile);
//                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand3);
//                snappyTest.executeProcess(pb, logFile);
            }
//            sleep(waitTimeBeforeJobStatus);
            snappyTest.getSnappyJobsStatus(snappyJobScript, logFile);
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
        }
    }

    protected void verifyDataForJobExecution(Vector jobClassNames, String userAppJar) {
        if (userAppJar == null) {
            String s = "Missing userAppJar parameter.";
            throw new TestException(s);
        }
        if (jobClassNames == null) {
            String s = "Missing JobClassNames parameter for required TASK/CLOSETASK.";
            throw new TestException(s);
        }
    }

    protected void executeSnappyJob(Vector jobClassNames, String logFileName) {
        String snappyJobScript = getScriptLocation("snappy-job.sh");
        ProcessBuilder pb = null;
        File log = null, logFile = null;
        userAppJar = TestConfig.tab().stringAt(SnappyPrms.userAppJar);
        snappyTest.verifyDataForJobExecution(jobClassNames, userAppJar);
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                String userJob = (String) jobClassNames.elementAt(i);
                String APP_PROPS = "\"logFileName=" + logFileName + ",shufflePartitions="+SnappyPrms.getShufflePartitions()+"\"";
                String curlCommand1 = "curl --data-binary @" + snappyTest.getUserAppJarLocation(userAppJar) + " " + leadHost + ":8090/jars/myapp";
                String curlCommand2 = "curl -d " + APP_PROPS + " '" + leadHost + ":8090/jobs?appName=myapp&classPath=" + userJob + "'";
                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand1);
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + logFileName;
                logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand2);
                snappyTest.executeProcess(pb, logFile);
            }
//            sleep(waitTimeBeforeJobStatus);
            snappyTest.getSnappyJobsStatus(snappyJobScript, logFile);
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
        }
    }

    /**
     * Returns the output file containing collective output for all threads executing Snappy job in CLOSETASK.
     */
    public static void HydraTask_getSnappyJobOutputCollectivelyForCloseTask() {
        snappyTest.getSnappyJobOutputCollectively("logFilesForCloseTask", "snappyJobCollectiveOutputForCloseTask.log");
    }

    /**
     * Returns the output file containing collective output for all threads executing Snappy job in TASK.
     */
    public static void HydraTask_getSnappyJobOutputCollectivelyForTask() {
        snappyTest.getSnappyJobOutputCollectively("logFilesForTask", "snappyJobCollectiveOutputForTask.log");
    }

    protected void getSnappyJobOutputCollectively(String logFilekey, String fileName) {
        Set<String> snappyJobLogFiles = new LinkedHashSet<String>();
        File fin = null;
        try {
            Set<String> keys = SnappyBB.getBB().getSharedMap().getMap().keySet();
            for (String key : keys) {
                if (key.startsWith(logFilekey)) {
                    String logFilename = (String) SnappyBB.getBB().getSharedMap().getMap().get(key);
                    Log.getLogWriter().info("Key Found...." + logFilename);
                    snappyJobLogFiles.add(logFilename);
                }
            }
            File dir = new File(".");
            String dest = dir.getCanonicalPath() + File.separator + fileName;
            File file = new File(dest);
            if (!file.exists()) return;
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.doneExecution);
            if (num == 1) {
                FileWriter fstream = new FileWriter(dest, true);
                BufferedWriter bw = new BufferedWriter(fstream);
                Iterator<String> itr = snappyJobLogFiles.iterator();
                while (itr.hasNext()) {
                    String userScript = itr.next();
                    String threadID = userScript.substring(userScript.lastIndexOf("_"), userScript.indexOf("."));
                    String threadInfo = "Thread" + threadID + " output:";
                    bw.write(threadInfo);
                    bw.newLine();
                    String fileInput = snappyTest.getLogDir() + File.separator + userScript;
                    fin = new File(fileInput);
                    FileInputStream fis = new FileInputStream(fin);
                    BufferedReader in = new BufferedReader(new InputStreamReader(fis));
                    String line = null;
                    while ((line = in.readLine()) != null) {
                        bw.write(line);
                        bw.newLine();
                    }
                    in.close();
                }
                bw.close();
            }
        } catch (FileNotFoundException e) {
            String s = "Unable to find file: " + fin;
            throw new TestException(s);
        } catch (IOException e) {
            String s = "Problem while writing to the file : " + fin;
            throw new TestException(s, e);
        }
    }

    protected void simulateStream() {
        File logFile = null;
        File log = new File(".");
        try {
            String streamScriptName = snappyTest.getScriptLocation(simulateStreamScriptName);
            ProcessBuilder pb = new ProcessBuilder(streamScriptName, simulateStreamScriptDestinationFolder, productDir);
            String dest = log.getCanonicalPath() + File.separator + "simulateFileStreamResult.log";
            logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            String s = "problem occurred while retriving destination logFile path " + log;
            throw new TestException(s, e);
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
                    jobIds.add(jobID);
                }
            }
            inputFile.close();
            for (String str : jobIds) {
                ProcessBuilder pb = new ProcessBuilder(snappyJobScript, "status", "--lead", leadHost + ":8090", "--job-id", str);
                snappyTest.executeProcess(pb, logFile);
            }
        } catch (FileNotFoundException e) {
            String s = "Unable to find file: " + logFile;
            throw new TestException(s);
        } catch (IOException e) {
            String s = "Problem while reading the file : " + logFile;
            throw new TestException(s, e);
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
            Vector<String> names = TestConfig.tab().vecAt(ClientPrms.gemfireNames);
            String dirname = hd.getUserDir() + File.separator
                    + "vm_" + RemoteTestModule.getMyVmid()
                    + "_" + RemoteTestModule.getMyClientName()
                    + "_" + HostHelper.getLocalHost()
                    + "_" + RemoteTestModule.getMyPid();
//            this.localHost = HostHelper.getLocalHost();
            File dir = new File(dirname);
            String fullname = dir.getAbsolutePath();
            try {
                FileUtil.mkdir(dir);
                try {
                    for (String name : names) {
                        String[] splitedName = name.split("gemfire");
                        String newName = splitedName[0] + splitedName[1];
                        if (newName.equals(RemoteTestModule.getMyClientName())) {
                            RemoteTestModule.Master.recordDir(hd,
                                    name, fullname);
                        }
                    }
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
            this.logFile = dirname;
            log().info("logFile name is " + this.logFile);
        }
        return this.logFile;
    }

    protected synchronized void generateConfig(String fileName) {
        File file = null;
        try {
            String path = productConfDirPath + sep + fileName;
            log().info("File Path is ::" + path);
            file = new File(path);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            } else if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
        } catch (IOException e) {
            String s = "Problem while creating the file : " + file;
            throw new TestException(s, e);
        }
    }

    public static void HydraTask_deleteSnappyConfig() throws IOException {
        String locatorConf = productConfDirPath + sep + "locators";
        String serverConf = productConfDirPath + sep + "servers";
        String leadConf = productConfDirPath + sep + "leads";
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

    /**
     * Create and start snappy locator.
     */
    public static synchronized void HydraTask_createAndStartSnappyLocator() {
        File log = null;
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.locatorsStarted);
            if (num == 1) {
                ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "start");
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "snappyLocatorSystem.log";
                File logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                snappyTest.recordSnappyProcessIDinNukeRun("LocatorLauncher");
            }
        } catch (IOException e) {
            String s = "problem occurred while retriving destination logFile path " + log;
            throw new TestException(s, e);
        }
    }

    /**
     * Create and start snappy server.
     */
    public static synchronized void HydraTask_createAndStartSnappyServers() {
        File log = null;
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.serversStarted);
            if (num == 1) {
                ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "start");
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
                File logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                snappyTest.recordSnappyProcessIDinNukeRun("ServerLauncher");
            }
        } catch (IOException e) {
            String s = "problem occurred while retriving logFile path " + log;
            throw new TestException(s, e);
        }
    }

    /**
     * Creates and start snappy lead.
     */
    public static synchronized void HydraTask_createAndStartSnappyLeader() {
        File log = null;
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.leadsStarted);
            if (num == 1) {
                ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"), "start");
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "snappyLeaderSystem.log";
                File logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                snappyTest.recordSnappyProcessIDinNukeRun("LeaderLauncher");
            }
        } catch (IOException e) {
            String s = "problem occurred while retriving logFile path " + log;
            throw new TestException(s, e);
        }
    }

    /**
     * Stops snappy lead.
     */
    public static synchronized void HydraTask_stopSnappyLeader() {
        File log = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-leads.sh"), "stop");
            log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "snappyLeaderSystem.log";
            File logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            String s = "problem occurred while retriving logFile path " + log;
            throw new TestException(s, e);
        }
    }

    /**
     * Stops snappy server/servers.
     */
    public static synchronized void HydraTask_stopSnappyServers() {
        File log = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-servers.sh"), "stop");
            log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
            File logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            String s = "problem occurred while retriving logFile path " + log;
            throw new TestException(s, e);
        }
    }

    /**
     * Stops a snappy locator.
     */
    public static synchronized void HydraTask_stopSnappyLocator() {
        File log = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-locators.sh"), "stop");
            log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "snappyLocatorSystem.log";
            File logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            String s = "problem occurred while retriving destination logFile path " + log;
            throw new TestException(s, e);
        }
    }

    public static synchronized void HydraTask_stopSnappyCluster() {
        File log = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(snappyTest.getScriptLocation("snappy-stop-all.sh"));
            log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + "snappySystem.log";
            File logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
        } catch (IOException e) {
            String s = "problem occurred while retriving destination logFile path " + log;
            throw new TestException(s, e);
        }
    }

    protected LogWriter log() {
        return Log.getLogWriter();
    }

}
