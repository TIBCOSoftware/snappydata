package io.snappydata.hydra.cdcConnector;

import hydra.Log;

import io.snappydata.hydra.cluster.SnappyTest;
import java.io.*;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

public class SnappyCDCTest extends SnappyTest {
  protected static SnappyCDCTest snappyCDCTest;

  public SnappyCDCTest() {
  }


  public static Connection getConnections() {
    Connection conn = null;
    List<String> endpoints = validateLocatorEndpointData();
    String hostPort = endpoints.get(0);
    String url = "jdbc:snappydata://" + hostPort;
    Log.getLogWriter().info("url is " + url);
    String driver = "io.snappydata.jdbc.ClientDriver";
    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
    } catch (Exception ex) {
      System.out.println("Caught exception in getConnection() method" + ex.getMessage());
    }

    return conn;
  }

  public static void HydraTask_runConcurrencyJob() {
    Log.getLogWriter().info("Inside HydraTask_runConcurrencyJob");
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
     }
     snappyCDCTest.runConcurrencyTestJob();
  }

  public static void HydraTask_closeStreamingJob() {
    String curlCmd = null;
    ProcessBuilder pb = null;
    String appName = SnappyCDCPrms.getAppName();
    String logFileName = "sparkStreamingStopResult_" + System.currentTimeMillis() + ".log";
    File log = null;
    File logFile = null;
    Log.getLogWriter().info("Inside HydraTask_closeStreamingJob");
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    try{
      InetAddress myHost = InetAddress.getLocalHost();
      String hostName[] = myHost.toString().split("/");
      curlCmd = "curl -d \"name="+appName+"&terminate=true\" -X POST http://"+hostName[0]+":8080/app/killByName/";
      Log.getLogWriter().info("The curlCmd  is " + curlCmd);
      pb = new ProcessBuilder("/bin/bash", "-c", curlCmd);
      log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + logFileName;
      logFile = new File(dest);
      snappyTest.executeProcess(pb, logFile);
    }
    catch(Exception ex){
      Log.getLogWriter().info("Exception in HydraTask_closeStreamingJob() "+ ex.getMessage());
    }
  }

  public void runConcurrencyTestJob() {
    try {
      CDCPerfSparkJob cdcPerfSparkJob = new CDCPerfSparkJob();
      List<String> endpoints = validateLocatorEndpointData();
      int threadCnt = SnappyCDCPrms.getThreadCnt();
      String queryPath = SnappyCDCPrms.getDataLocation();
      Boolean isScanQuery = SnappyCDCPrms.getIsScanQuery();
      Boolean isBulkDelete = SnappyCDCPrms.getIsBulkDelete();
      Boolean isPointLookUp = SnappyCDCPrms.getIsPointLookUP();
      Boolean isMixedQuery = SnappyCDCPrms.getIsMixedQuery();
      String sqlServerInst = SnappyCDCPrms.getSqlServerInstance();
      String dataBaseName = SnappyCDCPrms.getDataBaseName();
      int intStartRange = SnappyCDCPrms.getInitStartRange();
      Log.getLogWriter().info("Inside runConcurrencyTestJob() parameters are  " + threadCnt + " "+  queryPath+ " "
          +endpoints.get(0)+ " " + isScanQuery+ " " +isBulkDelete+ " " +isPointLookUp + " " + intStartRange + " "
          + sqlServerInst + " " + dataBaseName);
      cdcPerfSparkJob.runConcurrencyTestJob(threadCnt, queryPath,endpoints.get(0), isScanQuery,isBulkDelete,
          isPointLookUp,isMixedQuery,intStartRange,sqlServerInst);

    } catch (Exception ex) {
      Log.getLogWriter().info("Caught Exception" + ex.getMessage() + " in runConcurrencyTestJob() method");
    }
  }

  public void deleteDiskStore() {

  }

  public static void performClusterRestartWithNewNode() {
    String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
    String nodeInfoforHA = SnappyCDCPrms.getNodeInfoforHA();
    File bkName = null;
    File orgName = null;
    String query = "SELECT count(*) from AIRLINE";
    try{
      Connection conn = getConnections();
      ResultSet rs = conn.createStatement().executeQuery(query);
      Log.getLogWriter().info("Count value is " + rs.getInt(0));
      conn.close();
      ProcessBuilder pbStop = new ProcessBuilder(snappyPath + "/sbin/snappy-stop-all.sh");
      snappyTest.executeProcess(pbStop,null);
      bkName = new File(snappyPath + "/conf/servers_bk");
      orgName = new File(snappyPath + "/conf/servers");
      if (orgName.renameTo(bkName)) {
        Log.getLogWriter().info("File renamed to " + bkName);
      } else {
        Log.getLogWriter().info("Error while renaming file");
      }
      File newServerConf = new File (snappyPath + "/conf/servers");
      FileWriter fw = new FileWriter(newServerConf,true);
      fw.write(nodeInfoforHA);
      FileReader reader = new FileReader(bkName);
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        fw.write(line);
      }
      reader.close();
      FileReader reader1 = new FileReader(newServerConf);
      BufferedReader bufferedReader1 = new BufferedReader(reader1);
      String line1;
      while ((line1 = bufferedReader1.readLine()) != null) {
        System.out.println(line1);
      }
      reader1.close();

      Thread.sleep(60000);
      ProcessBuilder pbStart = new ProcessBuilder(snappyPath + "/sbin/snappy-start-all.sh");
      snappyTest.executeProcess(pbStart,null);
      Thread.sleep(60000);
      Connection conn1 = getConnections();
      ResultSet rs1 = conn1.createStatement().executeQuery(query);
      Log.getLogWriter().info("Count value is " + rs1.getInt(0));
      conn1.close();

      //Read the existing server config
    }
    catch(Exception e){}
  }

  public static void performHA() {
    File orgName = null;
    File bkName = null;
    File tempConfFile = null;
    String scriptName = "";
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      Log.getLogWriter().info("snappyPath File path is " + snappyPath);

      String nodeType = SnappyCDCPrms.getNodeType();

      if (nodeType.equals("allNodes")) {
        File log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "clusterRestart.log";
        Log.getLogWriter().info("The destination file is " + dest);
        File logFile = new File(dest);
        ProcessBuilder pbClustStop = new ProcessBuilder(snappyPath + "/sbin/snappy-stop-all.sh");
        Long startTime = System.currentTimeMillis();
        snappyTest.executeProcess(pbClustStop, logFile);
        Long totalTime = (System.currentTimeMillis() - startTime);
        Log.getLogWriter().info("The cluster took " + totalTime + " ms to shut down");

        //Restart the cluster after 10 mins
        Thread.sleep(600000);
        ProcessBuilder pbClustStart = new ProcessBuilder(snappyPath + "/sbin/snappy-start-all.sh");
        Long startTime1 = System.currentTimeMillis();
        snappyTest.executeProcess(pbClustStart, logFile);
        Long totalTime1 = (System.currentTimeMillis() - startTime1);
        Log.getLogWriter().info("The cluster took " + totalTime1 + " ms to shut down");
      } else {
        if(nodeType.equalsIgnoreCase("servers"))
          scriptName = "/sbin/snappy-servers.sh";
        else if(nodeType.equalsIgnoreCase("leads"))
          scriptName = "/sbin/snappy-leads.sh";
       else
         scriptName = "/sbin/snappy-locators.sh";

        orgName = new File(snappyPath + "/conf/" + nodeType);
        bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");

        String nodeInfoforHA = SnappyCDCPrms.getNodeInfoforHA();

        //rename original conf file
        if (orgName.renameTo(bkName)) {
          Log.getLogWriter().info("File renamed to " + bkName);
        } else {
          Log.getLogWriter().info("Error");
        }

        //write a new file in conf
        try {
          tempConfFile = new File(snappyPath + "/conf/" + nodeType);
          FileWriter fw = new FileWriter(tempConfFile);
          fw.write(nodeInfoforHA);
          fw.close();
          File log = new File(".");
          String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
          Log.getLogWriter().info("The destination file is " + dest);
          File logFile = new File(dest);

          Log.getLogWriter().info("The nodeType is " +nodeType+ " script to stop is " + scriptName);
          ProcessBuilder pbStop = new ProcessBuilder(snappyPath + scriptName, "stop");
          snappyTest.executeProcess(pbStop, logFile);

          Thread.sleep(30000); //sleep for 3 min before restarting the node.

          Log.getLogWriter().info("The nodeType is " +nodeType+ " script to start is " + scriptName);
          ProcessBuilder pbStart = new ProcessBuilder(snappyPath + scriptName, "start");
          snappyTest.executeProcess(pbStart, logFile);

          Thread.sleep(60000);

          //delete the temp conf file created.
          if (tempConfFile.delete()) {
            System.out.println(tempConfFile.getName() + " is deleted!");
          } else {
            System.out.println("Delete operation is failed.");
          }

          //restore the back up to its originals.
          if (bkName.renameTo(orgName)) {
            Log.getLogWriter().info("File renamed to " + orgName);
          } else {
            Log.getLogWriter().info("Error");
          }
        } catch (FileNotFoundException e) {
          // File not found
          e.printStackTrace();
        } catch (IOException e) {
          // Error when writing to the file
          e.printStackTrace();
        }
      }

    } catch (Exception e) {
      Log.getLogWriter().info("Caught Exception in performHA " + e.getMessage());
    }
  }

}
