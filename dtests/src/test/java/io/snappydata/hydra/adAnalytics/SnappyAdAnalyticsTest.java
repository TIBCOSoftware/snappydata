/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata.hydra.adAnalytics;


import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import hydra.HostHelper;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.hydra.streaming_sink.StringMessageProducer;
import io.snappydata.hydra.testDMLOps.DerbyTestUtils;
import org.apache.commons.io.FileUtils;
import util.TestException;

public class SnappyAdAnalyticsTest extends SnappyTest {

  public static String kafkaDir = TestConfig.tab().stringAt(SnappyPrms.kafkaDir, null);
  public static String snappyPocJarPath = TestConfig.tab().stringAt(SnappyPrms.snappyPocJarPath, null);
  protected static SnappyAdAnalyticsTest snappyAdAnalyticsTest;
  public static String zookeeperHost = null;
  public static String zookeeperPort = null;
  public static String kafkaLogDir = null;
  public static int initialBrokerPort = 9092;
  public static int initialBrokerId = 0;

  public SnappyAdAnalyticsTest() {
  }

  public static String getCurrentDirPath(){
    String currentDir;
    try {
      currentDir = new File(".").getCanonicalPath();
    } catch (IOException e) {
      String s = "Problem while accessing the current dir.";
      throw new TestException(s, e);
    }
    return currentDir;
  }

  public static synchronized void HydraTask_initializeSnappyAdAnalyticsTest() {
    if (snappyAdAnalyticsTest == null)
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
    if (kafkaDir == null) {
      String s = "Didnot specify kafka directory.";
      throw new TestException(s);
    }
    kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    new File(kafkaLogDir).mkdir();
    snappyAdAnalyticsTest.writeSnappyPocToSparkEnv();
  }

  protected void writeSnappyPocToSparkEnv() {
    String filePath = productConfDirPath + sep + "spark-env.sh";
    File file = new File(filePath);
    file.setExecutable(true);
    String fileContent = "SPARK_DIST_CLASSPATH=" + snappyPocJarPath;
    snappyAdAnalyticsTest.writeToFile(fileContent, file,true);
  }

  /**
   * Start kafka zookeeper.
   */
  public static synchronized void HydraTask_StartKafkaZookeeper() {
    snappyAdAnalyticsTest.startZookeeper();
  }

  protected void startZookeeper() {
    ProcessBuilder pb = null;
    try {
      String zookeeperLogDirPath = kafkaLogDir + sep + "zookeeper";
      new File(zookeeperLogDirPath).mkdir();
      String dest = zookeeperLogDirPath + sep + "zookeeperSystem.log";
      File logFile = new File(dest);

      String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/zookeeper-server-start.sh");
      String orgPropFilePath = snappyTest.getScriptLocation(kafkaDir + sep + "config/zookeeper.properties");
      File orgPropFile = new File(orgPropFilePath);

      String myPropFilePath = zookeeperLogDirPath + sep + "zookeeper.properties";
      File myPropFile = new File(myPropFilePath);
      //create copy of properties file to modify
      FileUtils.copyFile(orgPropFile, myPropFile);
      // change log dir in properperty file
      modifyPropFile(myPropFile, "dataDir", zookeeperLogDirPath);

      String command = "nohup " + script + " " + myPropFilePath + " > " + logFile + " &";
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      recordSnappyProcessIDinNukeRun("QuorumPeerMain");

      Log.getLogWriter().info("Started Kafka zookeeper");

      //Store zookeeper host and port on blackboard
      updateBlackboard(myPropFile, "clientPort");
    } catch (IOException e) {
      String s = "Problem while copying properties file.";
      throw new TestException(s, e);
    }
  }

  // save zookeeper host and port details on blackboard
  protected void updateBlackboard(File file, String searchString) {
    try {
      zookeeperHost = HostHelper.getIPAddress().getLocalHost().getHostName();
      SnappyBB.getBB().getSharedMap().put("zookeeperHost", zookeeperHost);
      FileInputStream fis = new FileInputStream(file);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String str = null;
      boolean found = false;
      while (!found && (str = br.readLine()) != null) {
        if (str.contains(searchString)) {
          found = true;
        }
      }
      br.close();
      if (found) {
        zookeeperPort = str.split("=")[1];
        SnappyBB.getBB().getSharedMap().put("zookeeperPort", zookeeperPort);
      } else
        throw new TestException("Didnot find port for zookeeper");
    } catch (IOException e) {
      String s = "Problem while reading the file : " + file;
      throw new TestException(s, e);
    }
  }

  /**
   * Start kafka brokers.
   */
  public static synchronized void HydraTask_StartKafkaBrokers() {
    snappyAdAnalyticsTest.startKafkaBroker();
  }

  protected void startKafkaBroker() {
    String command;
    int numServers = (int)SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
    Log.getLogWriter().info("Test will start " + numServers + " kafka brokers.");
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-server-start.sh");
    String orgPropFilePath = snappyTest.getScriptLocation(kafkaDir + sep + "config/server.properties");
    File orgPropFile = new File(orgPropFilePath);
    try {
      for (int i = 1; i <= numServers; i++) {
        String broker = "broker" + i;
        String brokerLogDirPath = kafkaLogDir + sep + broker;
        new File(brokerLogDirPath).mkdir();

        String dest = brokerLogDirPath + sep + "brokerSystem.log";
        File logFile = new File(dest);

        String myPropFilePath = brokerLogDirPath + sep + broker + ".properties";
        File myPropFile = new File(myPropFilePath);
        //create copy of properties file to modify
        FileUtils.copyFile(orgPropFile, myPropFile);

        //change port and logdir for servers
        modifyPropFile(myPropFile,"log.dir", brokerLogDirPath);
        modifyPropFile(myPropFile,"port=",Integer.toString(initialBrokerPort));
        modifyPropFile(myPropFile,"broker.id",Integer.toString(initialBrokerId++));
        Log.getLogWriter().info(broker + " properties files is  " + myPropFile);

        command = "nohup " + script + " " + myPropFilePath + " > " + logFile + " &";
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        snappyTest.executeProcess(pb, logFile);

        Log.getLogWriter().info("Started kafka " + broker);
        if (i == 1)
          SnappyBB.getBB().getSharedMap().put("brokerList", "localhost--" + initialBrokerPort);
        initialBrokerPort = initialBrokerPort + 2;
      }
    }  catch (IOException e) {
    String s = "Problem while copying properties file.";
    throw new TestException(s, e);
  }
    recordSnappyProcessIDinNukeRun("Kafka");
  }

  //change log directory and port for property file
  protected void modifyPropFile(File file, String searchString, String replaceString) {
    String str = null;
    ArrayList<String> lines = new ArrayList<String>();
    try {
      FileInputStream fis = new FileInputStream(file);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      while ((str = br.readLine()) != null) {
        if (str.trim().startsWith(searchString)) {
          str = str.replace(str.split("=")[1], replaceString);
          Log.getLogWriter().info("File str is ::" + str);
        }
        lines.add(str + "\n");
      }
      br.close();
      fis.close();
      FileWriter fw = new FileWriter(file);
      for (String line : lines)
        fw.write(line);
      fw.close();
    } catch (IOException e) {
      String s = "Problem while reading the file : " + file;
      throw new TestException(s, e);
    }
  }

  /**
   * Start kafka topic.
   */

  public static void HydraTask_StartKafkaTopic() {
    snappyAdAnalyticsTest.startKafkaTopic(SnappyPrms.getKafkaTopic());
  }

  protected void startKafkaTopic(Vector topics) {
    ProcessBuilder pb = null;

    zookeeperHost = (String)SnappyBB.getBB().getSharedMap().get("zookeeperHost");
    zookeeperPort = (String)SnappyBB.getBB().getSharedMap().get("zookeeperPort");

    for (int i = 0; i < topics.size(); i++) {
      String topic = (String)topics.elementAt(i);
      String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-topics.sh");
      String command = script + " --create --zookeeper " + zookeeperHost + ":" + zookeeperPort +
          " --partition 8 --topic " + topic + " --replication-factor=1";

      String dest = kafkaLogDir + sep + "startTopic-" + topic + ".log";
      File logFile = new File(dest);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      Log.getLogWriter().info("Started Kafka topic: " + topic);
    }
  }

  public static void HydraTask_executeSnappyStreamingJob() {
    snappyAdAnalyticsTest.executeSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames(),
        "snappyStreamingJobTaskResult_" + System.currentTimeMillis() + ".log");
  }

  protected void executeSnappyStreamingJob(Vector jobClassNames, String logFileName) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    String APP_PROPS = "";
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;
    String userJarPath = getUserAppJarLocation(SnappyPrms.getUserAppJar(),jarPath);
    verifyDataForJobExecution(jobClassNames, userJarPath);
    leadHost = getLeadHost();
    String brokerList = null;
    String leadPort = getLeadPort();
    try {
      for (int i = 0; i < jobClassNames.size(); i++) {
        String userJob = (String)jobClassNames.elementAt(i);
        if (SnappyPrms.getCommaSepAPPProps() != null) {
          String[] appProps = SnappyPrms.getCommaSepAPPProps().split(",");
          for (int j = 0; j < appProps.length; j++)
            APP_PROPS = APP_PROPS + " --conf " + appProps[j];
        }
        brokerList = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
        APP_PROPS = APP_PROPS + " --conf brokerList=" + brokerList + " --conf tid=" + getMyTid();
        String appName = SnappyPrms.getUserAppName();
        Log.getLogWriter().info("APP PROPS :" + APP_PROPS);
        String snappyJobCommand = snappyJobScript + " submit --lead " + leadHost + ":" + leadPort +
            " --app-name " + appName + " --class " + userJob + " --app-jar " + userJarPath +
            APP_PROPS + " --stream ";
        String dest = getCurrentDirPath() + File.separator + logFileName;
        Log.getLogWriter().info("Executing cmd:" + snappyJobCommand);
        logFile = new File(dest);
        pb = new ProcessBuilder("/bin/bash", "-c", snappyJobCommand);
        snappyTest.executeProcess(pb, logFile);
        String line = null;
        String jobID = null;
        BufferedReader inputFile = new BufferedReader(new FileReader(logFile));
        while ((line = inputFile.readLine()) != null) {
          if (line.contains("jobId")) {
            jobID = line.split(":")[1].trim();
            jobID = jobID.substring(1, jobID.length() - 2);
            break;
          }
        }
        inputFile.close();
        if(jobID == null){
          throw new TestException("Failed to start the streaming job. Please check the logs.");
        }
        Log.getLogWriter().info("JobID is : " + jobID);
        for (int j = 0; j < 3; j++) {
          try {
            Thread.sleep(10 * 1000);
          } catch (InterruptedException ie) {
          }
          getJobStatus(jobID);
        }
        if(!checkJobStatus(jobID)){
          throw new TestException("Got Exception while executing streaming job. Please check " +
              "the job status output.");
        }
      }
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
    }
  }

  public void getJobStatus(String jobID) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    leadHost = getLeadHost();
    String leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    String dest = getCurrentDirPath() + File.separator + "jobStatus_" + getMyTid() + "_" + jobID +
        ".log";
    File commandOutput = new File(dest);
    String command = snappyJobScript + " status --lead " + leadHost + ":" + leadPort + " " +
        "--job-id " + jobID + " > " + commandOutput;
    ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
    executeProcess(pb, null);
  }

  public static void HydraTask_executeSQLScriptsWithSleep() {
    try { Thread.sleep(30000); } catch (InterruptedException ie) {}
    HydraTask_executeSQLScripts();
  }


  public static void HydraTask_restartStreaming() {
    HydraTask_stopStreamingJob();
    try { Thread.sleep(60000); } catch (InterruptedException ie) {}
    HydraTask_executeSnappyStreamingJob();
    try { Thread.sleep(30000); } catch (InterruptedException ie) {}
  }

  public static void HydraTask_restartLeadVMWithStreaming(){
    HydraTask_cycleLeadVM();
    try { Thread.sleep(180000); } catch (InterruptedException ie) {}
    HydraTask_executeSnappyStreamingJob();
  }

  public static void HydraTask_restartSnappyClusterForStreaming(){
   HydraTask_stopSnappyCluster();
   HydraTask_startSnappyCluster();
   HydraTask_executeSnappyStreamingJob();
  }

  /* Generator and Publisher for StringMessageProducer
  */
  public static void HydraTask_generateAndPublishMethod() {
    String[] appProps = null;
    if (SnappyPrms.getCommaSepAPPProps() != null) {
      appProps = SnappyPrms.getCommaSepAPPProps().split(",");
    }
    String[] appProps1 = Arrays.copyOf(appProps, appProps.length + 3);
    appProps1[appProps.length] = "" + TestConfig.tasktab().booleanAt(SnappyPrms.isConflationTest,
        false);
    appProps1[appProps.length + 1] = "" + DerbyTestUtils.hasDerbyServer;
    appProps1[appProps.length + 2] = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
    StringMessageProducer.generateAndPublish(appProps1);
  }

  /**
   * Start generating and publishing logs to Kafka
   */
  public static void HydraTask_generateAndPublish() {
    snappyAdAnalyticsTest.generateAndPublish(SnappyPrms.getSnappyStreamingJobClassNames());
  }

  protected void generateAndPublish(Vector generatorAndPublisher) {
    ProcessBuilder pb = null;
    String APP_PROPS = "";
    String userJarPath = getUserAppJarLocation(SnappyPrms.getUserAppJar(),jarPath);
    if (SnappyPrms.getCommaSepAPPProps() != null) {
      String[] appProps = SnappyPrms.getCommaSepAPPProps().split(",");
      for (int j = 0; j < appProps.length; j++)
        APP_PROPS = APP_PROPS + " " + appProps[j];
    }
    boolean hasDerby = false;
    String brokerList = (String)SnappyBB.getBB().getSharedMap().get("brokerList");
    APP_PROPS += " " + hasDerby + " " + brokerList;
    for (int i = 0; i < generatorAndPublisher.size(); i++) {
      String dest = kafkaLogDir + sep + "generatorAndPublisher" + getMyTid() + "_" + i + ".log";
      File logFile = new File(dest);
      String processName= (String)generatorAndPublisher.elementAt(i);
      String command = "java -cp .:" + userJarPath + ":" + getStoreTestsJar() + ":" + productDir +
          "jars/* " +  processName + " " + APP_PROPS + " > " + logFile;
      if(SnappyPrms.executeInBackGround())
        command = "nohup " + command + " & ";
      Log.getLogWriter().info("Executing cmd : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      recordSnappyProcessIDinNukeRun(processName);
    }
  }

  public static void HydraTask_calculateStreamingTime(){
    List<String> tableName = SnappyPrms.getTableList();
    List<String> expectedRows = SnappyPrms.getNumRowsList();
    snappyAdAnalyticsTest.calculateStreamingTime(tableName.get(0),Integer.parseInt(expectedRows.get(0)));
  }

  public void calculateStreamingTime(String tableName, int expectedRows){
    Long startTime = System.currentTimeMillis();
    Long endTime = 0L;
    Connection conn = null;
    boolean allConsumed = false;
    int numRows = 0;
    try {
      conn = getLocatorConnection();
    } catch(SQLException se) {
      throw new TestException("Got exception while getting connection.",se);
    }
    try {
      while (!allConsumed) {
        ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + tableName);
        while (rs.next()) {
          numRows = rs.getInt(1);
        }
        Log.getLogWriter().info("Number of records returned are : " + numRows);
        if (numRows == expectedRows) {
          allConsumed = true;
          endTime = System.currentTimeMillis();
        }
      }
      Log.getLogWriter().info("Total time for consuming streaming data is :" + (endTime -
          startTime)/1000);
    } catch(SQLException se) {
      throw new TestException("Got exception while quering the table.",se);
    }

  }
  public static void HydraTask_assertStreaming(){
    List<String> tableName = SnappyPrms.getTableList();
    List<String> expectedRows = SnappyPrms.getNumRowsList();
    snappyAdAnalyticsTest.assertStreaming(tableName.get(0),Integer.parseInt(expectedRows.get(0)));
  }

  public void assertStreaming(String tableName, int expectedRows){
    Connection conn = null;
    int numRows = 0;
    try {
      conn = getLocatorConnection();
    } catch(SQLException se) {
      throw new TestException("Got exception while getting connection.",se);
    }
    try {
      ResultSet rs= conn.createStatement().executeQuery("select count(*) from " + tableName);
      while(rs.next()) {
        numRows = rs.getInt(1);
      }
      if(numRows != expectedRows){
        throw new TestException("Streaming app didnot consume all the data for " + tableName +
            " from the stream. Expected " + expectedRows + " rows to be consumed, but got only "
            + numRows + "rows.");
      }
    } catch(SQLException se) {
      throw new TestException("Got exception while quering the table.",se);
    }

  }

  /**
   * Stop kafka brokers.
   */
  public static synchronized void HydraTask_StopKafkaBrokers() {
    if(snappyAdAnalyticsTest == null) {
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    }
    snappyAdAnalyticsTest.stopKafkaBroker();
  }

  protected void stopKafkaBroker() {
    File log = null;
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-server-stop.sh");
    String dest = kafkaLogDir + sep + "stopKafkaServer.log";
    File logFile = new File(dest);
    pb = new ProcessBuilder("/bin/bash", "-c", script);
    snappyTest.executeProcess(pb, logFile);
    Log.getLogWriter().info("Stopped Kafka servers");

  }

  /**
   * Stop kafka brokers.
   */

  public static synchronized void HydraTask_StopKafkaZookeeper() {
    if(snappyAdAnalyticsTest == null) {
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    }
    snappyAdAnalyticsTest.stopKafkaZookeeper();
  }

  protected void stopKafkaZookeeper() {
    File log = null;
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/zookeeper-server-stop.sh");
    String dest = kafkaLogDir + sep + "stopZookeeper.log";
    File logFile = new File(dest);
    pb = new ProcessBuilder("/bin/bash", "-c", script);
    snappyTest.executeProcess(pb, logFile);
    Log.getLogWriter().info("Stopped Kafka zookeeper");
  }

  public static void HydraTask_stopStreamingJob() {
    Vector jobClassNames = SnappyPrms.getSnappyJobClassNames();
    if(jobClassNames == null){
      jobClassNames = SnappyPrms.getSnappyStreamingJobClassNames();
    }
    snappyAdAnalyticsTest.stopSnappyStreamingJob(jobClassNames);
  }

  protected void stopSnappyStreamingJob(Vector jobClassNames) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;
    String userJarPath = getUserAppJarLocation(SnappyPrms.getUserAppJar(), jarPath);
    verifyDataForJobExecution(jobClassNames, userJarPath);
    leadHost = getLeadHost();
    String leadPort = (String)SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    String userJob = (String)jobClassNames.elementAt(0);
    String snappyJobCommand = snappyJobScript + " submit --lead " + leadHost + ":" + leadPort +
        " --app-name AdAnalytics --class " + userJob + " --app-jar " + userJarPath;
    Log.getLogWriter().info("Executing cmd :" + snappyJobCommand);
    String dest = getCurrentDirPath() + File.separator + "stopSnappyStreamingJobTaskResult.log";
    logFile = new File(dest);
    pb = new ProcessBuilder("/bin/bash", "-c", snappyJobCommand);
    snappyTest.executeProcess(pb, logFile);
  }

}
