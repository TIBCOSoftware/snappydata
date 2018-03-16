package io.snappydata.hydra.cdcConnector;

import hydra.Log;
import io.snappydata.hydra.cluster.SnappyTest;

import java.util.List;

public class SnappyCDCTest extends SnappyTest {
  protected static SnappyCDCTest snappyCdcTest;

  public SnappyCDCTest() {
  }

  public static void HydraTask_runIngestionApp() {
    if (snappyCdcTest == null) {
      snappyCdcTest = new SnappyCDCTest();
    }
    snappyCdcTest.runIngestionApp();
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

  public void runIngestionApp() {
    CDCIngestionApp cdcIngestionApp = new CDCIngestionApp();
    int start = SnappyCDCPrms.getStartRange();
    int end = SnappyCDCPrms.getEndRange();
    cdcIngestionApp.runIngestionApp(start, end);
  }

  public void runConcurrencyTestJob() {
    try {
      CDCPerfSparkJob cdcPerfSparkJob = new CDCPerfSparkJob();
      List<String> endpoints = validateLocatorEndpointData();
      int threadCnt = SnappyCDCPrms.getThreadCnt();
      String path = SnappyCDCPrms.getDataLocation();
      Boolean isScanQuery = SnappyCDCPrms.getIsScanQuery();
      Log.getLogWriter().info("Inside runConcurrencyTestJob() " + threadCnt + " " + path + " "+isScanQuery + " hostPort = " + endpoints.get(0));
      cdcPerfSparkJob.runConcurrencyTestJob(threadCnt, path, isScanQuery,endpoints.get(0));

    } catch (Exception ex) {
      Log.getLogWriter().info("Caught Exception" + ex.getMessage() + " in runConcurrencyTestJob() method");
    }
  }

}
