package io.snappydata.hydra.cluster;


import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;

import hydra.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
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

    private static transient JavaSparkContext jsc;
    private static transient SnappyContext snc;
    protected static SnappyTest snappyTest;
    private static char sep;
    private static HostDescription hd;
    private static String snappyPath = null;
    private static String logFile = null;
    private static String localHost = null;

    private static Set<String> serversFileContent = new LinkedHashSet<String>();
    private static Set<String> locatorsFileContent = new LinkedHashSet<String>();
    private static Set<String> leadsFileContent = new LinkedHashSet<String>();
    private static Set<String> snappyJobLogFilesForTask = new LinkedHashSet<String>();
    private static Set<String> snappyJobLogFilesForCloseTask = new LinkedHashSet<String>();
    private static Set<Integer> pids = new LinkedHashSet<Integer>();
    private static String locatorsFilePath = null;
    private static String serversFilePath = null;
    private static String leadsFilePath = null;
    private static String userAppJar = null;
    private static String simulateStreamScriptName = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptName, "simulateFileStream");
    private static String simulateStreamScriptDestinationFolder = null;
    private static String leadHost = null;
    public static Long waitTimeBeforeJobStatusInTask = TestConfig.tab().longAt(SnappyPrms.jobExecutionTimeInMillisForTask, 6000);
    public static Long waitTimeBeforeStreamingJobStatusInTask = TestConfig.tab().longAt(SnappyPrms.streamingJobExecutionTimeInMillisForTask, 6000);
    public static Long waitTimeBeforeJobStatusInCloseTask = TestConfig.tab().longAt(SnappyPrms.jobExecutionTimeInMillisForCloseTask, 6000);
    private static Boolean logDirExists = false;


    public static <A, B> Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>>conforms());
    }

    public static void HydraTask_initializeSnappy() {
        SparkContext sc = null;
        snc = SnappyContext.getOrCreate(sc);
        Log.getLogWriter().info("SnappyContext initialized successfully");
    }


    public static void HydraTask_stopSnappy() {
        SnappyContext.stop(true);
        Log.getLogWriter().info("SnappyContext stopped successfully");
    }

    protected String getDataLocation() {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "quickstart" + sep + "data";
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
        return hd;
    }

    protected String getScriptLocation(String scriptName) {
        String scriptPath = null;
        try {
            hd = getHostDescription();
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + "snappy"
                    + sep + "sbin" + sep + scriptName;
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
        return scriptPath;
    }

    protected String getSnappyPath() {
        hd = getHostDescription();
        snappyPath = hd.getGemFireHome() + sep + ".." + sep + "snappy";
        return snappyPath;
    }

    protected String getParamPath(String paramName) {
        try {
            String fullPath = null;
            if (paramName.contains("/")) {
                return paramName;
            } else {
                fullPath = snappyTest.getDataLocation() + sep + paramName;
                File path = new File(fullPath);
                if (path.exists()) {
                    Log.getLogWriter().info("Data File path found under quickstart data location.");
                } else {
                    fullPath = snappyTest.getUserDataLocation(paramName);
                    Log.getLogWriter().info("Data file path found under dtests data location.");
                }
            }
            return fullPath;
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }

    }

    protected String getUserScriptLocation(String scriptName) {
        String scriptPath = null;
        try {
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + ".." + sep + ".." + sep + "dtests" + sep + "src" + sep + "resources" + sep + "scripts" + sep + scriptName;
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
        return scriptPath;
    }

    protected String getUserDataLocation(String scriptName) {
        String scriptPath = null;
        try {
            scriptPath = hd.getGemFireHome() + sep + ".." + sep + ".." + sep + ".." + sep + "dtests" + sep + "src" + sep + "resources" + sep + "data" + sep + scriptName;
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
        return scriptPath;
    }

    protected String getUserAppJarLocation(String userAppJar) {
        String jarPath = null;
        try {
            jarPath = hd.getGemFireHome() + sep + ".." + sep + ".." + sep + ".." + sep + "dtests" + sep + "build-artifacts" + sep + "scala-2.10" + sep + "libs" + sep + userAppJar;
        } catch (Exception e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
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

    public static synchronized void HydraTask_generateSnappyConfig() throws NullPointerException {
        if (logDirExists) return;
        else {
            locatorsFilePath = snappyTest.getSnappyPath() + sep + "conf" + sep + "locators";
            serversFilePath = snappyTest.getSnappyPath() + sep + "conf" + sep + "servers";
            leadsFilePath = snappyTest.getSnappyPath() + sep + "conf" + sep + "leads";
            String addr = HostHelper.getHostAddress();
            int port = PortHelper.getRandomPort();
            String endpoint = addr + ":" + port;
            String clientPort = " -client-port=";
            String locators = "-locators=";
            String locatorHost = null;
            String dirPath = snappyTest.getLogDir();
            File locatorsFile = new File(locatorsFilePath);
            File serversFile = new File(serversFilePath);
            File leadsFile = new File(leadsFilePath);
            if (dirPath.contains("locator")) {
                String locatorLogDir = localHost + " -dir=" + dirPath + clientPort + port;
                locatorsFileContent.add(locatorLogDir);
                String portString = port + "";
                SnappyBB.getBB().getSharedMap().put("locatorHost", localHost);
                SnappyBB.getBB().getSharedMap().put("locatorPort", portString);
                snappyTest.writeToFile(locatorLogDir, locatorsFile);
                Log.getLogWriter().info("Generated peer locator endpoint: " + endpoint);
                SnappyNetworkServerBB.getBB().getSharedMap().put("locator" + "_" + RemoteTestModule.getMyVmid(), endpoint);
            } else if (dirPath.contains("Store") || dirPath.contains("server")) {
                locatorHost = (String) SnappyBB.getBB().getSharedMap().get("locatorHost");
                String serverLogDir = localHost + " " + locators + locatorHost + ":" + 10334 + " -dir=" + dirPath + clientPort + port;
                Log.getLogWriter().info("serverLogDir" + serverLogDir);
                serversFileContent.add(serverLogDir);
                snappyTest.writeToFile(serverLogDir, serversFile);
                Log.getLogWriter().info("Generated peer server endpoint: " + endpoint);
                SnappyNetworkServerBB.getBB().getSharedMap().put("server" + "_" + RemoteTestModule.getMyVmid(), endpoint);
            } else if (dirPath.contains("lead")) {
                locatorHost = (String) SnappyBB.getBB().getSharedMap().get("locatorHost");
                String leadLogDir = localHost + " " + locators + locatorHost + ":" + 10334 + " -dir=" + dirPath + clientPort + port;
                leadsFileContent.add(leadLogDir);
                if (leadHost == null) {
                    leadHost = localHost;
                }
                snappyTest.writeToFile(leadLogDir, leadsFile);
            }
            logDirExists = true;
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
        String url = "jdbc:snappydata://" + endpoints.get(0); //+ "/";
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
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }

    /**
     * Executes user scripts for InitTask.
     */
    public static void HydraTask_executeSQLScriptsInInitTask() {
        Vector scriptNames, paramList = null;
        scriptNames = SnappyPrms.getSQLScriptNamesForInitTask();
        paramList = SnappyPrms.getSQLScriptParamsForInitTask();
        try {
            for (int i = 0; i < scriptNames.size(); i++) {
                String userScript = (String) scriptNames.elementAt(i);
                String fileName = userScript;
                String param = (String) paramList.elementAt(i);
                String path = snappyTest.getParamPath(param);
                String SnappyShellPath = snappyTest.getSnappyPath() + sep + "bin" + sep + "snappy-shell";
                String filePath = snappyTest.getUserScriptLocation(fileName);
                File file = new File(filePath);
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "sqlScriptsInitTaskResult.log";
                File logFile = new File(dest);
                int clientPort = snappyTest.getClientPort();
                ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" + file, "-param:path=" + path, "-client-port=" + clientPort);
                snappyTest.executeProcess(pb, logFile);
            }
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
            assert pb.redirectInput() == ProcessBuilder.Redirect.PIPE;
            assert pb.redirectOutput().file() == logFile;
            assert p.getInputStream().read() == -1;
            int rc = p.waitFor();
        } catch (Exception e1) {
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    protected void recordSnappyProcessIDinNukeRun(String pName) {
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
            Process pr = pb.start();
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
        scriptNames = SnappyPrms.getSQLScriptNamesForTask();
        try {
            for (int i = 0; i < scriptNames.size(); i++) {
                String userScript = (String) scriptNames.elementAt(i);
                String fileName = userScript;
                String SnappyShellPath = snappyTest.getSnappyPath() + sep + "bin" + sep + "snappy-shell";
                String filePath = snappyTest.getUserScriptLocation(fileName);
                File file = new File(filePath);
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + "sqlScriptsTaskResult.log";
                File logFile = new File(dest);
                int clientPort = snappyTest.getClientPort();
                ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" + file, "-client-port=" + clientPort);
                snappyTest.executeProcess(pb, logFile);
            }
        } catch (Exception e1) {
            snappyTest.removeSnappyProcessIDinNukeRun();
            throw new TestException("Following error occurred while executing " + e1.getMessage());
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
//            snappyTest.getSnappyJobOutputCollectively("logFilesForCloseTask", snappyJobLogFilesForCloseTask, "snappyJobCollectiveOutputForCloseTask.log");
        } catch (InterruptedException e) {
            throw new TestException("Following error occurred while executing HydraTask_executeSnappyJobInCloseTask" + e.getMessage());
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
//            snappyTest.getSnappyJobOutputCollectively("logFilesForTask", snappyJobLogFilesForTask, "snappyJobCollectiveOutputForTask.log");
        } catch (InterruptedException e) {
            throw new TestException("Following error occurred while executing HydraTask_executeSnappyJobInTask" + e.getMessage());
        }
    }

    /**
     * Executes snappy Streaming Jobs in Task.
     */

    public static void HydraTask_executeSnappyStreamingJob() {
        Runnable fileStreaming = new Runnable() {
            public void run() {
                try {
                    snappyTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNamesForTask(), "snappyJobTaskResult.log");
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
            Log.getLogWriter().info("Sleeping for " + waitTimeBeforeStreamingJobStatusInTask + "millis before executor service shut down");
            Thread.sleep(waitTimeBeforeStreamingJobStatusInTask);
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
        userAppJar = TestConfig.tab().stringAt(SnappyPrms.userAppJar);
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                String userJob = (String) jobClassNames.elementAt(i);
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
        userAppJar = TestConfig.tab().stringAt(SnappyPrms.userAppJar);
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                String userJob = (String) jobClassNames.elementAt(i);
                String APP_PROPS = "\"logFileName=" + logFileName + "\"";
                String curlCommand1 = "curl --data-binary @" + snappyTest.getUserAppJarLocation(userAppJar) + " " + leadHost + ":8090/jars/myapp";
                String curlCommand2 = "curl -d " + APP_PROPS + " '" + leadHost + ":8090/jobs?appName=myapp&classPath=" + userJob + "'";
                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand1);
                File log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + logFileName;
                logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand2);
                snappyTest.executeProcess(pb, logFile);
            }
//            sleep(waitTimeBeforeJobStatus);
            snappyTest.getSnappyJobsStatus(snappyJobScript, logFile);
        } catch (Exception e1) {
            snappyTest.removeSnappyProcessIDinNukeRun();
            throw new TestException("Following error occurred while executing " + e1.getMessage());
        }
    }

    public static void HydraTask_getSnappyJobOutputCollectively() {
        snappyTest.getSnappyJobOutputCollectively("logFilesForCloseTask", snappyJobLogFilesForCloseTask, "snappyJobCollectiveOutputForCloseTask.log");
    }

    protected void getSnappyJobOutputCollectively(String logFilekey, Set<String> snappyJobLogFiles, String fileName) {
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
//            SnappyBB.getBB().getSharedLock().lock();
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
                    File fin = new File(fileInput);
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
//            SnappyBB.getBB().getSharedLock().unlock();
            }
        } catch (IOException e) {
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
    }

    protected void simulateStream() {
        String fileName = "simulateFileStreamResult.log";
        ProcessBuilder pb = null;
        File logFile = null;
        simulateStreamScriptDestinationFolder = TestConfig.tab().stringAt(SnappyPrms.simulateStreamScriptDestinationFolder);
        String streamScriptName = snappyTest.getUserScriptLocation(simulateStreamScriptName);
        String command = streamScriptName;
        try {
            pb = new ProcessBuilder(command, simulateStreamScriptDestinationFolder, snappyTest.getSnappyPath());
            File log = new File(".");
            String dest = log.getCanonicalPath() + File.separator + fileName;
            logFile = new File(dest);
            snappyTest.executeProcess(pb, logFile);
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
                    jobIds.add(jobID);
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
//            String name = TestConfig.tab().stringAt(ClientPrms.gemfireNames, "gemfire1");
            Vector<String> names = TestConfig.tab().vecAt(ClientPrms.gemfireNames);
            String dirname = hd.getUserDir() + File.separator
                    + "vm_" + RemoteTestModule.getMyVmid()
                    + "_" + RemoteTestModule.getMyClientName()
                    + "_" + HostHelper.getLocalHost()
                    + "_" + RemoteTestModule.getMyPid();
            this.localHost = HostHelper.getLocalHost();
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
            throw new TestException("Following error occurred while executing " + e.getMessage());
        }
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

    /**
     * Create and start snappy locator.
     */
    public static synchronized void HydraTask_createAndStartSnappyLocator() {
        try {
            int num = (int) SnappyBB.getBB().getSharedCounters().incrementAndRead(SnappyBB.locatorsStarted);
            if (num == 1) {
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
