package io.snappydata.hydra.cdcConnector;

import akka.io.Tcp;
import hydra.*;

import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyStartUpTest;
import io.snappydata.hydra.cluster.SnappyTest;
import io.snappydata.test.util.TestException;

import java.io.*;

import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SnappyCDCTest extends SnappyTest {
  public static SnappyCDCTest snappyCDCTest;

  public SnappyCDCTest() {
  }

  public static Connection getSnappyConnection() {
    Connection conn = null;
    List<String> endpoints = validateLocatorEndpointData();
    String url = "jdbc:snappydata://"+endpoints;
    String driver = "io.snappydata.jdbc.ClientDriver";
    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
    } catch (Exception ex) {
      System.out.println("Caught exception in getSnappyConnection() method" + ex.getMessage());
    }
    return conn;
  }

   public static void meanKillProcesses(){
     Process pr = null;
     ProcessBuilder pb;
     File logFile, log = null, serverKillOutput;
     Set<Integer> pids = new LinkedHashSet<>();
     Set<String> pidList = new LinkedHashSet<>();
     try {
       HostDescription hd = TestConfig.getInstance().getMasterDescription()
           .getVmDescription().getHostDescription();
       pidList = SnappyStartUpTest.getServerPidList();
       log = new File(".");
       String server = log.getCanonicalPath() + File.separator + "server.sh";
       logFile = new File(server);
       String serverKillLog = log.getCanonicalPath() + File.separator + "serverKill.log";
       serverKillOutput = new File(serverKillLog);
       FileWriter fw = new FileWriter(logFile.getAbsoluteFile(), true);
       BufferedWriter bw = new BufferedWriter(fw);
       List asList = new ArrayList(pidList);
       Collections.shuffle(asList);
       String pidString = String.valueOf(asList.get(0));
       Log.getLogWriter().info("pidString : " + pidString);
       int pid = Integer.parseInt(pidString);
       Log.getLogWriter().info("Server Pid chosen for abrupt kill : " + pidString);
       String pidHost = snappyTest.getPidHost(Integer.toString(pid));
       if (pidHost.equalsIgnoreCase("localhost")) {
         bw.write("/bin/kill -KILL " + pid);
       } else {
         bw.write("ssh -n -x -o PasswordAuthentication=no -o StrictHostKeyChecking=no " +
             pidHost + " /bin/kill -KILL " + pid);
       }
       bw.newLine();
       try {
         RemoteTestModule.Master.removePID(hd, pid);
       } catch (RemoteException e) {
         String s = "Failed to remove PID from nukerun script: " + pid;
         throw new HydraRuntimeException(s, e);
       }
       bw.close();
       fw.close();
       logFile.setExecutable(true);
       pb = new ProcessBuilder(server);
       pb.redirectErrorStream(true);
       pb.redirectOutput(ProcessBuilder.Redirect.appendTo(serverKillOutput));

       //wait for 30 secs before issuing mean kill
       Thread.sleep(5000);
       pr = pb.start();
       pr.waitFor();
     } catch (IOException e) {
       throw new util.TestException("IOException occurred while retriving logFile path " + log + "\nError Message:" + e.getMessage());
     } catch (InterruptedException e) {
       String s = "Exception occurred while waiting for the process execution : " + pr;
       throw new util.TestException(s, e);
     }
  }

  public static void performRebalance(){
     try {
       Connection conn = getSnappyConnection();
       conn.createStatement().execute("call sys.rebalance_all_buckets();");
     }
     catch(SQLException ex){

     }
  }

  public static void addNewNode() {
    Log.getLogWriter().info("Indside startNewNodeFirst method");
    String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
    Log.getLogWriter().info("snappyPath File path is " + snappyPath);
    Boolean isNewNodeFirst = SnappyCDCPrms.getIsNewNodeFirst();
    String nodeType = SnappyCDCPrms.getNodeType();

    File orgName = new File(snappyPath + "/conf/" + nodeType);
    File bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");
    File newFile = new File(snappyPath + "/conf/" + nodeType + "_nw");

    if (orgName.renameTo(bkName)) {
      Log.getLogWriter().info("File renamed to " + bkName);
    } else {
      Log.getLogWriter().info("Error");
    }
    String nodeConfig = SnappyCDCPrms.getNodeConfig();
    try {
      if (!isNewNodeFirst) {
        Connection conn = getSnappyConnection();
        FileWriter fw = new FileWriter(orgName, true);
        fw.write(nodeConfig);

        //restart the cluster
        clusterRestart(snappyPath,false,nodeType);
      } else {
        if (nodeType.equalsIgnoreCase("servers")) {
          FileWriter fw = new FileWriter(newFile);
          fw.write(nodeConfig);
          if (fw != null)
            fw.close();

          //  File fin = new File(snappyPath + "/conf/" + nodeType);
          FileInputStream fis = new FileInputStream(orgName);
          BufferedReader br = new BufferedReader(new InputStreamReader(fis));

          FileWriter fw1 = new FileWriter(newFile, true);
          BufferedWriter bw = new BufferedWriter(fw1);

          String aLine = null;
          while ((aLine = br.readLine()) != null) {
            bw.write(aLine);
            bw.newLine();
          }
          br.close();
          bw.close();

          //rename new file to its original.
          if (newFile.renameTo(orgName)) {
            Log.getLogWriter().info("File renamed to " + orgName);
          } else {
            Log.getLogWriter().info("Error");
          }

          //delete the temp conf file created.
          if (bkName.delete()) {
            System.out.println(bkName.getName() + " is deleted!");
          } else {
            System.out.println("Delete operation is failed.");
          }


          clusterRestart(snappyPath, false, nodeType);
        } else if (nodeType.equalsIgnoreCase("leads")) {
          clusterRestart(snappyPath, true, nodeType);
        }
      }

    } catch (FileNotFoundException e) {
      // File not found
      e.printStackTrace();
    } catch (IOException e) {
      // Error when writing to the file
      e.printStackTrace();
    } catch (Exception e) {
      Log.getLogWriter().info("Caught Exception in performHA " + e.getMessage());
    }
  }

  public static void clusterRestart(String snappyPath, Boolean isStopStart, String nodeType) {
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterRestart.log";
      Log.getLogWriter().info("The destination file is " + dest);
      File logFile = new File(dest);
      //Stop cluster
      if (isStopStart) {
        ProcessBuilder pbClustStop = new ProcessBuilder(snappyPath + "/sbin/snappy-stop-all.sh");
        Long startTime = System.currentTimeMillis();
        snappyTest.executeProcess(pbClustStop, logFile);
        Long totalTime = (System.currentTimeMillis() - startTime);
        Log.getLogWriter().info("The cluster took " + totalTime + " ms to shut down");
      }
      if (nodeType.equalsIgnoreCase("leads")) {
        //start locator:
        ProcessBuilder pb = new ProcessBuilder(snappyPath + "/sbin/snappy-locators.sh", "start");
        snappyTest.executeProcess(pb, logFile);
        //start leader:
        ProcessBuilder pb1 = new ProcessBuilder(snappyPath + "/sbin/snappy-leads.sh", "start");
        snappyTest.executeProcess(pb1, logFile);
      }

      //Start the cluster after 1 min
      Thread.sleep(60000);
      ProcessBuilder pbClustStart = new ProcessBuilder(snappyPath + "/sbin/snappy-start-all.sh");
      Long startTime1 = System.currentTimeMillis();
      snappyTest.executeProcess(pbClustStart, logFile);
      Long totalTime1 = (System.currentTimeMillis() - startTime1);
      Log.getLogWriter().info("The cluster took " + totalTime1 + " ms to shut down");
    } catch (IOException ex) {
    } catch (Exception ex) {
    }
  }

  public static void storeDataCount(){
    Log.getLogWriter().info("Inside storeDataCount method");
    int dataCnt = 0;
    try {
    Connection con = getSnappyConnection();
    String tableCntQry = "SELECT COUNT(*) FROM SYS.SYSTABLES WHERE TABLESCHEMANAME='APP'";
    ResultSet rs = con.createStatement().executeQuery(tableCntQry);
    String[] tableArr = new String [ rs.getInt(1)];
    Map<String,Integer> tableCntMap = new HashMap<>();
    int cnt = 0;
      String tableQry = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLESCHEMANAME='APP'";
      ResultSet rs1 = con.createStatement().executeQuery(tableQry);
      while(rs1.next())
      {
        String tableName = rs1.getString(1);
        tableArr[cnt] = tableName;
        cnt++;
        Log.getLogWriter().info("The array cnt = " + cnt + " and table name is = " +tableName);
      }
      for(int i = 0;i<tableArr.length;i++){
        String tableName = tableArr[i];
        String cntQry = "SELECT COUNT(*) FROM " + tableName;
        ResultSet rs3 = con.createStatement().executeQuery(cntQry);
        int count = rs3.getInt(1);
        tableCntMap.put(tableName,count);
      }
      SnappyBB.getBB().getSharedMap().put("tableCntMap",tableCntMap);
    }
    catch(SQLException ex){
      Log.getLogWriter().info("Caught exception in validateDataCount() " + ex.getMessage() + " SQL State is " + ex.getSQLState());
    }
  }

  public static void validateDataCount(){
    Log.getLogWriter().info("Inside validateDataCount method");
    Map<String,Integer> tableCntMap = (Map<String,Integer>)SnappyBB.getBB().getSharedMap().get("tableCntMap");
    try {
      Connection con = getSnappyConnection();
      for (Map.Entry<String, Integer> val : tableCntMap.entrySet()) {
        String tableName = val.getKey();
        int BBCnt = val.getValue();
        String cntQry = "SELECT COUNT(*) FROM " + tableName;
        ResultSet rs3 = con.createStatement().executeQuery(cntQry);
        int snappyCnt = rs3.getInt(1);
        if(snappyCnt == BBCnt)
          Log.getLogWriter().info("SUCCESS : The cnt for table " + tableName + " = "+snappyCnt+ " is EQUAL to the BB count = "+ BBCnt);
        else
          Log.getLogWriter().info("FAILURE :The cnt fot table " + tableName + " = "+snappyCnt+ " is NOT EQUAL to the BB count = " + BBCnt);
      }
    }
    catch(SQLException ex) {}
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

      String nodeInfoforHA = SnappyCDCPrms.getNodeConfig();

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
