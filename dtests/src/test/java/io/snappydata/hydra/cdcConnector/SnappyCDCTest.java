package io.snappydata.hydra.cdcConnector;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.spark.sql.catalyst.plans.logical.Except;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class SnappyCDCTest extends SnappyTest {
  protected static SnappyCDCTest snappyCdcTest;

  public SnappyCDCTest() {
  }

  private static Properties readPropertyFile(String filePath) throws Exception {
    File file = new File(filePath);
    FileInputStream fileInput = new FileInputStream(file);
    Properties properties = new Properties();
    properties.load(fileInput);
    fileInput.close();
    return properties;
  }
  public static void HydraTask_runConcurrencyJob() {
    Log.getLogWriter().info("Inside HydraTask_runConcurrencyJob");
    if (snappyCdcTest == null) {
      snappyCdcTest = new SnappyCDCTest();
      Log.getLogWriter().info("SP1");
    }
    Log.getLogWriter().info("SP2");
    snappyCdcTest.runConcurrencyTestJob();
    Log.getLogWriter().info("SP3");
  }

  public static void HydraTask_closeStreamingJob() {
    String curlCmd = null;
    ProcessBuilder pb = null;
    String appName = SnappyCDCPrms.getAppName();
    String logFileName = "sparkStreamingStopResult_" + System.currentTimeMillis() + ".log";
    File log = null;
    File logFile = null;
    Log.getLogWriter().info("Inside HydraTask_closeStreamingJob");
    if (snappyCdcTest == null) {
      snappyCdcTest = new SnappyCDCTest();
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

 /* public void runIngestionApp() {
    CDCIngestionApp cdcIngestionApp = new CDCIngestionApp();
    int start = SnappyCDCPrms.getStartRange();
    int end = SnappyCDCPrms.getEndRange();
    Log.getLogWriter().info("Inside runIngestionApp");
    cdcIngestionApp.runIngestionApp(start, end);
    Log.getLogWriter().info("Finish runIngestionApp");
  }*/


  public void runConcurrencyTestJob() {
    try {
      CDCPerfSparkJob cdcPerfSparkJob = new CDCPerfSparkJob();
      List<String> endpoints = validateLocatorEndpointData();
      int threadCnt = SnappyCDCPrms.getThreadCnt();
      String queryPath = SnappyCDCPrms.getDataLocation();
      Boolean isScanQuery = SnappyCDCPrms.getIsScanQuery();
      Boolean isBulkDelete = SnappyCDCPrms.getIsBulkDelete();
      int startRange = SnappyCDCPrms.getStartRange();
      Log.getLogWriter().info("Inside runConcurrencyTestJob() " + threadCnt + " " + queryPath + " "+isScanQuery + " hostPort = " + endpoints.get(0));
      cdcPerfSparkJob.runConcurrencyTestJob(threadCnt, queryPath,endpoints.get(0), isScanQuery,isBulkDelete,startRange);

    } catch (Exception ex) {
      Log.getLogWriter().info("Caught Exception" + ex.getMessage() + " in runConcurrencyTestJob() method");
    }
  }

}
