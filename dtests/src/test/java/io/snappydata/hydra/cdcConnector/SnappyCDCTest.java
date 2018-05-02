package io.snappydata.hydra.cdcConnector;

import hydra.Log;

import io.snappydata.hydra.cluster.SnappyTest;

import java.io.*;

import java.util.List;
import java.util.Properties;

public class SnappyCDCTest extends SnappyTest {
  protected static SnappyCDCTest snappyCDCTest;

  public SnappyCDCTest() {
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
      curlCmd = "curl -d \"name="+appName+"&terminate=true\" -X POST http://pnq-spillai3:8080/app/killByName/";
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

  public static void performHA() {
    File orgName = null;
    File bkName = null;
    File tempConfFile = null;
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      Log.getLogWriter().info("snappyPath File path is " + snappyPath);

      String nodeType = SnappyCDCPrms.getNodeType();

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
        ProcessBuilder pbStop = new ProcessBuilder(snappyPath + "/sbin/snappy-servers.sh", "stop");
        snappyTest.executeProcess(pbStop, logFile);

        Thread.sleep(30000); //sleep for 3 min before restarting the node.

        ProcessBuilder pbStart = new ProcessBuilder(snappyPath + "/sbin/snappy-servers.sh", "start");
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

    } catch (Exception e) {
      Log.getLogWriter().info("Caught Exception in performHA " + e.getMessage());
    }
  }

}
