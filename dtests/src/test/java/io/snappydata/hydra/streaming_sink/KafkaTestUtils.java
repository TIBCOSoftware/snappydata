package io.snappydata.hydra.streaming_sink;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Vector;

import hydra.BasePrms;
import hydra.HostPrms;
import hydra.Log;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.commons.io.FileUtils;
import util.TestException;

public class KafkaTestUtils extends SnappyTest{

  public static String kafkaDir = TestConfig.tab().stringAt(SnappyPrms.kafkaDir, null);
  public static String zookeeperHost = null;
  public static String zookeeperPort = null;
  public static String kafkaLogDir = TestConfig.tab().stringAt(SnappyPrms.kafkaLogDir, null);
  public static int initialBrokerPort = 9092;
  public static int initialBrokerId = 0;
  public static String[] hostnames;

  public static KafkaTestUtils kafkaTestInstance = null;

  public static void HydraTask_initialize(){
    if(kafkaTestInstance == null)
      kafkaTestInstance = new KafkaTestUtils();

    if (kafkaDir == null) {
      String s = "Did not specify kafka directory.";
      throw new TestException(s);
    }
    if(kafkaLogDir==null)
      kafkaLogDir = SnappyTest.getCurrentDirPath() + sep + "kafka_logs";
    new File(kafkaLogDir).mkdir();
  }

  public static String[] getNames(Long key) {
    Vector vec = BasePrms.tab().vecAt(key, null);
    if(vec == null) return null;
    String[] strArr = new String[vec.size()];
    for (int i = 0; i < vec.size(); i++) {
      strArr[i] = (String)vec.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }

  public static String[] getHostNames() {

    String[] vmNames = getNames(HostPrms.names);
    String[] vmHostNames = null;
    int numServers=0;
    if(SnappyTest.isUserConfTest) {
      vmHostNames = getNames(SnappyPrms.hostNames);
      SnappyBB.getBB().getSharedCounters().zero(SnappyBB.numServers);
      SnappyBB.getBB().getSharedCounters().setIfLarger(SnappyBB.numServers,vmHostNames.length);
      Log.getLogWriter().info("The vmHostNames size = " + vmHostNames.length);
    }
    else {
      vmHostNames = getNames(HostPrms.hostNames);
    }
    numServers = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
    Log.getLogWriter().info("Number of servers = " + numServers);
    hostnames = new String[numServers];
    if(vmHostNames==null) {
      for (int j = 0; j<numServers ;  j++)
        hostnames[j] = "localhost";
    } else {
      int j = 0;
      for (int i = 0; i < vmNames.length; i++) {
        if (SnappyTest.isUserConfTest || vmNames[i].startsWith("snappyStore")) {
          hostnames[j] = vmHostNames[i];
          Log.getLogWriter().info("Host name is " + hostnames[j]);
          j++;
        }
      }
    }
    return hostnames;
  }

  /**
   * Start kafka zookeeper.
   */
  public static synchronized void HydraTask_StartKafkaZookeeper() {
    kafkaTestInstance.startZookeeper();
  }

  protected void startZookeeper() {
    ProcessBuilder pb = null;
    try {
      getHostNames();
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
      zookeeperHost = hostnames[0];
      modifyPropFile(myPropFile, "dataDir=", zookeeperLogDirPath);
      modifyPropFile(myPropFile,"host.name=", zookeeperHost);
      String command = "";
      if(!zookeeperHost.equals("localhost"))
        command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + zookeeperHost;
      command = "nohup " + command + " " + script + " " + myPropFilePath + " > " + logFile + " &";
      Log.getLogWriter().info("Executing command : " + command);
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
    kafkaTestInstance.startKafkaBroker();
  }

  protected void startKafkaBroker() {
    String command = "";
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
        String hostname = hostnames[i-1];
        //change port and logdir for servers
        modifyPropFile(myPropFile,"log.dirs=", brokerLogDirPath);
        modifyPropFile(myPropFile,"zookeeper.connect=",zookeeperHost+":"+ zookeeperPort);
        modifyPropFile(myPropFile,"host.name=",hostname);
        modifyPropFile(myPropFile,"port=",Integer.toString(initialBrokerPort));
        modifyPropFile(myPropFile,"broker.id=",Integer.toString(initialBrokerId++));
        Log.getLogWriter().info(broker + " properties files is  " + myPropFile);
        if(!hostname.equals("localhost"))
          command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostname;
        command = "nohup " + command + " " + script + " " + myPropFilePath + " > " + logFile + " &";
        Log.getLogWriter().info("Executing command : " + command);
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command);
        snappyTest.executeProcess(pb, logFile);

        Log.getLogWriter().info("Started kafka " + broker);
        if (i == 1)
          SnappyBB.getBB().getSharedMap().put("brokerList", hostname + "--" + initialBrokerPort);
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
    boolean isReplaced = false;
    ArrayList<String> lines = new ArrayList<String>();
    try {
      FileInputStream fis = new FileInputStream(file);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      while ((str = br.readLine()) != null) {
        if (str.trim().startsWith(searchString)) {
          str = str.replace(str.split("=")[1], replaceString);
          isReplaced = true;
          Log.getLogWriter().info("File str is ::" + str);
        }
        lines.add(str + "\n");
      }
      if(!isReplaced)
        lines.add(searchString + replaceString);
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
    kafkaTestInstance.startKafkaTopic(SnappyPrms.getKafkaTopic());
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
      Log.getLogWriter().info("Executing command : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
      Log.getLogWriter().info("Started Kafka topic: " + topic);
    }
  }


  /**
   * Stop kafka brokers.
   */
  public static synchronized void HydraTask_StopKafkaBrokers() {
    if(kafkaTestInstance == null) {
      kafkaTestInstance = new KafkaTestUtils();
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    }
    kafkaTestInstance.stopKafkaBroker();
  }

  protected void stopKafkaBroker() {
    getHostNames();
    File log = null;
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/kafka-server-stop.sh");
    int numServers = (int)SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
    for (int i = 1; i <= numServers; i++) {
      String dest = kafkaLogDir + sep + "broker" + i + sep + "stopKafkaServer.log";
      File logFile = new File(dest);
      String hostname = hostnames[i-1];
      String command = "";
      if(!hostname.equals("localhost"))
        command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostname;
      command = command + "  " + script;
      Log.getLogWriter().info("Executing command : " + command);
      pb = new ProcessBuilder("/bin/bash", "-c", command);
      snappyTest.executeProcess(pb, logFile);
    }
    Log.getLogWriter().info("Stopped Kafka servers");
  }

  /**
   * Stop kafka zookeeper.
   */

  public static synchronized void HydraTask_StopKafkaZookeeper() {
    if(kafkaTestInstance == null) {
      kafkaTestInstance = new KafkaTestUtils();
      kafkaLogDir = getCurrentDirPath() + sep + "kafka_logs";
    }
    kafkaTestInstance.stopKafkaZookeeper();
  }

  protected void stopKafkaZookeeper() {
    File log = null;
    String command = "";
    ProcessBuilder pb = null;
    String script = snappyTest.getScriptLocation(kafkaDir + sep + "bin/zookeeper-server-stop.sh");
    String dest = kafkaLogDir + sep + "stopZookeeper.log";
    File logFile = new File(dest);
    String hostname = (String)SnappyBB.getBB().getSharedMap().get("zookeeperHost");
    if(!hostname.equals("localhost"))
      command = "ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " + hostname;
    command = command + " " + script;
    Log.getLogWriter().info("Executing command : " + command);
    pb = new ProcessBuilder("/bin/bash", "-c", command);
    snappyTest.executeProcess(pb, logFile);
    Log.getLogWriter().info("Stopped Kafka zookeeper");
  }

}
