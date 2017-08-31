/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Vector;

import hydra.HostHelper;
import hydra.Log;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.streaming.SnappyStreamingContext;
import org.apache.spark.streaming.api.java.JavaSnappyStreamingContext;
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

  public static synchronized void HydraTask_initializeSnappyAdAnalyticsTest() {
    if (snappyAdAnalyticsTest == null)
      snappyAdAnalyticsTest = new SnappyAdAnalyticsTest();
    if (kafkaDir == null) {
      String s = "Didnot specify kafka directory.";
      throw new TestException(s);
    }
    try {
      File log = null;
      log = new File(".");
      kafkaLogDir = log.getCanonicalPath() + sep + "kafka_logs";
      new File(kafkaLogDir).mkdir();
      snappyAdAnalyticsTest.writeSnappyPocToSparkEnv();
    } catch (IOException e) {
      String s = "Problem while creating the dir : " + kafkaLogDir;
      throw new TestException(s, e);
    }
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
      generatePropFile(myPropFile, "dataDir", zookeeperLogDirPath);

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
        generatePropFile(myPropFile,"log.dir", brokerLogDirPath);
        generatePropFile(myPropFile,"port=",Integer.toString(initialBrokerPort));
        initialBrokerPort = initialBrokerPort + 2;
        generatePropFile(myPropFile,"broker.id",Integer.toString(initialBrokerId++));
        Log.getLogWriter().info(broker + " properties files is  " + myPropFile);

        command = "nohup " + script + " " + myPropFilePath + " > " + logFile + " &";
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        snappyTest.executeProcess(pb, logFile);

        Log.getLogWriter().info("Started kafka " + broker);
      }
    }  catch (IOException e) {
    String s = "Problem while copying properties file.";
    throw new TestException(s, e);
  }
    recordSnappyProcessIDinNukeRun("Kafka");
  }

  //change log directory and port for property file
  protected void generatePropFile(File file, String searchString, String replaceString) {
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
    String APP_PROPS = null;
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;
    userAppJar = SnappyPrms.getUserAppJar();
    verifyDataForJobExecution(jobClassNames, userAppJar);
    leadHost = getLeadHost();
    String leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    try {
      for (int i = 0; i < jobClassNames.size(); i++) {
        String userJob = (String)jobClassNames.elementAt(i);
        if (SnappyPrms.getCommaSepAPPProps() == null) {
          APP_PROPS = "shufflePartitions=" + SnappyPrms.getShufflePartitions();
        } else {
          APP_PROPS = SnappyPrms.getCommaSepAPPProps() + ",shufflePartitions=" + SnappyPrms.getShufflePartitions();
        }
        String snappyJobCommand = snappyJobScript + " submit --lead " + leadHost + ":" + leadPort +
            " --app-name AdAnalytics --class " + userJob + " --app-jar " + userAppJar + " --stream ";
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + logFileName;
        logFile = new File(dest);
        pb = new ProcessBuilder("/bin/bash", "-c", snappyJobCommand);
        snappyTest.executeProcess(pb, logFile);
      }
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
    }
  }

  /**
   * Start generating and publishing logs to Kafka
   */

  public static void HydraTask_generateAndPublish() {
    snappyAdAnalyticsTest.generateAndPublish(SnappyPrms.getSnappyStreamingJobClassNames());
  }

  protected void generateAndPublish(Vector generatorAndPublisher) {
    ProcessBuilder pb = null;
    String dest = kafkaLogDir + sep + "generatorAndPublisher.log";
    File logFile = new File(dest);
    for (int i = 0; i < generatorAndPublisher.size(); i++) {
      String processName= (String)generatorAndPublisher.elementAt(i);
      String command = "nohup java -cp " + snappyPocJarPath + ":" + productDir + "/jars/* " +
          processName + " > " + logFile + " & ";
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      recordSnappyProcessIDinNukeRun(processName);
    }
  }

  /**
   * Stop kafka brokers.
   */
  public static synchronized void HydraTask_StopKafkaBrokers() {
    snappyAdAnalyticsTest.stopKafkaBroker();
  }

  protected void stopKafkaBroker() {
    File log = null;
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-server-stop.sh");
    log = new File(".");
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
    snappyAdAnalyticsTest.stopKafkaZookeeper();
  }

  protected void stopKafkaZookeeper() {
    File log = null;
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/zookeeper-server-stop.sh");
    log = new File(".");
    String dest = kafkaLogDir + sep + "stopZookeeper.log";
    File logFile = new File(dest);
    pb = new ProcessBuilder("/bin/bash", "-c", script);
    snappyTest.executeProcess(pb, logFile);
    Log.getLogWriter().info("Stopped Kafka zookeeper");
  }

  public static void HydraTask_stopStreamingJob() {
    snappyAdAnalyticsTest.stopSnappyStreamingJob(SnappyPrms.getSnappyStreamingJobClassNames());
  }

  protected void stopSnappyStreamingJob(Vector jobClassNames) {
    String snappyJobScript = getScriptLocation("snappy-job.sh");
    ProcessBuilder pb = null;
    File log = null;
    File logFile = null;
    userAppJar = SnappyPrms.getUserAppJar();
    verifyDataForJobExecution(jobClassNames, userAppJar);
    leadHost = getLeadHost();
    String leadPort = (String) SnappyBB.getBB().getSharedMap().get("primaryLeadPort");
    try {
        String userJob = (String)jobClassNames.elementAt(0);
        String snappyJobCommand = snappyJobScript + " submit --lead " + leadHost + ":" + leadPort +
            " --app-name AdAnalytics --class " + userJob + " --app-jar " + snappyTest.getUserAppJarLocation(userAppJar, jarPath);

        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "stopSnappyStreamingJobTaskResult.log";
        logFile = new File(dest);
        pb = new ProcessBuilder("/bin/bash", "-c", snappyJobCommand);
        snappyTest.executeProcess(pb, logFile);
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
    }
  }

}
