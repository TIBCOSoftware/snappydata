package io.snappydata.hydra.cdcConnector;

import com.gemstone.gemfire.GemFireConfigException;
import hydra.*;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyStartUpTest;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.InetAddress;
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
    String url = "jdbc:snappydata://"+endpoints.get(0);
    Log.getLogWriter().info("The URL is " + url);
    String driver = "io.snappydata.jdbc.ClientDriver";
    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url);
    } catch (Exception ex) {
      System.out.println("Caught exception in getSnappyConnection() method" + ex.getMessage());
    }
    return conn;
  }

  public static List getHostNames(){
    Set<String> pidList ;
    pidList = SnappyStartUpTest.getServerPidList();
    List asList = new ArrayList(pidList);
    return asList;
  }

   public static void meanKillProcesses(){
     Process pr = null;
     ProcessBuilder pb;
     File logFile, log = null, serverKillOutput;
     Set<String> pidList ;
     Boolean isStopStart = SnappyCDCPrms.getIsStopStartCluster();
     String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
     String nodeType = SnappyCDCPrms.getNodeType();
     int numNodes = SnappyCDCPrms.getNumNodes();
     String pidString ;
     try {
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
       for(int i=0;i<numNodes;i++) {
         pidString = String.valueOf(asList.get(i));
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
     clusterRestart(snappyPath,isStopStart,nodeType,false,"",false);
  }

  public static void performRebalance(){
     try {
       Connection conn = getSnappyConnection();
       Long startTime = System.currentTimeMillis();
       conn.createStatement().execute("call sys.rebalance_all_buckets();");
       Long endTime = System.currentTimeMillis();
       Long totalTime = endTime - startTime;
       Log.getLogWriter().info("The rebalance procedure took  " + totalTime + " ms");
     }
     catch(SQLException ex){
        throw new util.TestException("Caught exception in performRebalance() " + ex.getMessage());
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

    if (orgName.renameTo(bkName)) {
      Log.getLogWriter().info("File renamed to " + bkName);
    } else {
      Log.getLogWriter().info("Error");
    }
    String nodeConfig = SnappyCDCPrms.getNodeConfig();
    try {
      if (!isNewNodeFirst) {
        FileWriter fw = new FileWriter(orgName, true);
        fw.write(nodeConfig);
        //restart the cluster
        clusterRestart(snappyPath,false,nodeType,false,"",false);
      } else {
        File tempConfFile = new File(snappyPath + "/conf/" + nodeType);
        if (nodeType.equalsIgnoreCase("servers")) {
          FileWriter fw = new FileWriter(tempConfFile);
          fw.write(nodeConfig);
          if (fw != null)
            fw.close();
          FileInputStream fis = new FileInputStream(orgName);
          BufferedReader br = new BufferedReader(new InputStreamReader(fis));

          FileWriter fw1 = new FileWriter(tempConfFile, true);
          BufferedWriter bw = new BufferedWriter(fw1);
          String aLine = null;
          while ((aLine = br.readLine()) != null) {
            bw.write(aLine);
            bw.newLine();
          }
          br.close();
          bw.close();
          clusterRestart(snappyPath, false, nodeType,false,"",false);
        } else if (nodeType.equalsIgnoreCase("leads")) {
          clusterRestart(snappyPath, true, nodeType,false,"",false);
        }

        //delete the temp conf file created.
        if (tempConfFile.delete()) {
          System.out.println(tempConfFile.getName() + " is deleted!");
        } else {
          System.out.println("Delete operation is failed.");
        }

        //rename bk file to its original.
        if (bkName.renameTo(orgName)) {
          Log.getLogWriter().info("File renamed to " + orgName);
        } else {
          Log.getLogWriter().info("Error");
        }
      }

    } catch (FileNotFoundException e) {
      // File not found
      e.printStackTrace();
    } catch (IOException e) {
      // Error when writing to the file
      e.printStackTrace();
    } catch (Exception e) {
      Log.getLogWriter().info("Caught Exception in addNewNode " + e.getMessage());
    }
  }

  public static void clusterModifyAndRestart(String snappyPath,String nodeType,String nodeConfig){
    File orgName = new File(snappyPath + "/conf/" + nodeType);
    File bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");
    List hostList = getHostNames();
    if (orgName.renameTo(bkName)) {
      Log.getLogWriter().info("File renamed to " + bkName);
    } else {
      Log.getLogWriter().info("Error");
    }
    try {
      File tempConfFile = new File(snappyPath + "/conf/" + nodeType);
      FileWriter fw = new FileWriter(tempConfFile,true);
      for(int i=0;i<hostList.size();i++) {
        String pidString = String.valueOf(hostList.get(i));
        Log.getLogWriter().info("pidString : " + pidString);
        int pid = Integer.parseInt(pidString);
        Log.getLogWriter().info("Server Pid is : " + pidString);
        String pidHost = snappyTest.getPidHost(Integer.toString(pid));
        String newConfig = pidHost+ " " + nodeConfig+"/pidHost" + " -critical-heap-percentage=95 \n" ;
        fw.write(newConfig);
      }
      fw.close();
    }
    catch(IOException ex) {
      Log.getLogWriter().info("Caught exception in clusterModifyAndRestart " + ex.getMessage());
    }
  }

  public static void clusterRestart(String snappyPath, Boolean isStopStart, String nodeType,Boolean isBackupRecovery,String nodeConfig,Boolean isModifyConf) {
    try {
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterRestart.log";
      Log.getLogWriter().info("The destination file is " + dest);
      File logFile = new File(dest);
      String dirPath = SnappyCDCPrms.getDataLocation();
      //Stop cluster
      if (isStopStart) {
        stopCluster(snappyPath,logFile);
      }
      if (nodeType.equalsIgnoreCase("leads")) {
        //start locator:
        ProcessBuilder pb = new ProcessBuilder(snappyPath + "/sbin/snappy-locators.sh", "start");
        snappyTest.executeProcess(pb, logFile);
        //start leader:
        ProcessBuilder pb1 = new ProcessBuilder(snappyPath + "/sbin/snappy-leads.sh", "start");
        snappyTest.executeProcess(pb1, logFile);
      }
      if (nodeType.equalsIgnoreCase("servers"))
      {
        //only start servers first:
        ProcessBuilder pb1 = new ProcessBuilder(snappyPath + "/sbin/snappy-servers.sh", "start");
        snappyTest.executeProcess(pb1, logFile);
      }
      if (isBackupRecovery)
      {
        Log.getLogWriter().info("Inside isBackupRecovery true loop");
        removeDiskStoreFiles(dirPath);
      }
      if(isModifyConf){
        clusterModifyAndRestart(snappyPath,nodeType,nodeConfig);
      }
      //Start the cluster after 1 min
      Thread.sleep(60000);
      startCluster(snappyPath,logFile);
    }catch(GemFireConfigException ex ){
      Log.getLogWriter().info("Got the expected exception when starting a new server ,without locators " + ex.getMessage());
    }
    catch (IOException ex) {
      Log.getLogWriter().info("Caught exception in cluster restart " + ex.getMessage());
    } catch (Exception ex) {
      Log.getLogWriter().info("Caught exception during cluster restart " + ex.getMessage());
    }
  }

  public static void storeDataCount(){
    Log.getLogWriter().info("Inside storeDataCount method");
    int tableCnt = 0;
    try {
    Connection con = getSnappyConnection();
    String tableCntQry = "SELECT COUNT(*) FROM SYS.SYSTABLES WHERE TABLESCHEMANAME='APP' AND TABLENAME NOT LIKE 'SNAPPYSYS_INTERNA%'";
    ResultSet rs = con.createStatement().executeQuery(tableCntQry);
    while(rs.next())
       tableCnt = rs.getInt(1);
    rs.close();
    String[] tableArr = new String [tableCnt];
    Map<String,Integer> tableCntMap = new HashMap<>();
    int cnt = 0;
      String tableQry = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLESCHEMANAME='APP'";
      ResultSet rs1 = con.createStatement().executeQuery(tableQry);
      while(rs1.next())
      {
        String tableName = rs1.getString("TABLENAME");
        if(!tableName.contains("SNAPPYSYS_INTERNA")) {
          tableArr[cnt] = tableName;
          cnt++;
        }
      }
      rs1.close();
      for(int i = 0;i<tableArr.length;i++){
        int count = 0;
        String tableName = tableArr[i];
        String cntQry = "SELECT COUNT(*) FROM " + tableName;
        ResultSet rs3 = con.createStatement().executeQuery(cntQry);
        while(rs3.next())
          count = rs3.getInt(1);
        tableCntMap.put(tableName,count);
        rs3.close();
      }
      SnappyBB.getBB().getSharedMap().put("tableCntMap",tableCntMap);
      con.close();
    }
    catch(SQLException ex){
      Log.getLogWriter().info("Caught exception in storeDataCount() " + ex.getMessage() + " SQL State is " + ex.getSQLState());
    }
  }

  public static void validateDataCount(){
    Log.getLogWriter().info("Inside validateDataCount method");
    Map<String,Integer> tableCntMap = (Map<String,Integer>)SnappyBB.getBB().getSharedMap().get("tableCntMap");
    Log.getLogWriter().info("tableCntMap size = " + tableCntMap);
    try {
      Connection con = getSnappyConnection();
      for (Map.Entry<String, Integer> val : tableCntMap.entrySet()) {
        int snappyCnt =0;
        String tableName = val.getKey();
        int BBCnt = val.getValue();
        String cntQry = "SELECT COUNT(*) FROM " + tableName;
        Log.getLogWriter().info("");
        ResultSet rs3 = con.createStatement().executeQuery(cntQry);
        while(rs3.next())
          snappyCnt = rs3.getInt(1);
        rs3.close();
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

  public static void HydraTask_clusterRestart() {
    Log.getLogWriter().info("Inside HydraTask_clusterRestart");
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    Boolean isStopStart = SnappyCDCPrms.getIsStopStartCluster();
    String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
    String nodeType = SnappyCDCPrms.getNodeType();
    String nodeConfig = SnappyCDCPrms.getNodeConfig();
    Boolean isModifyConf = SnappyCDCPrms.getIsModifyConf();
    Boolean isBackUpRecovery = SnappyCDCPrms.getIsBackUpRecovery();
    snappyCDCTest.clusterRestart(snappyPath,isStopStart,nodeType,isBackUpRecovery,nodeConfig,isModifyConf);
  }

  public static void HydraTask_stopCluster() {
    Log.getLogWriter().info("Inside HydraTask_stopCluster");
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterStop.log";
      File logFile = new File(dest);
      snappyCDCTest.stopCluster(snappyPath,logFile);
    }
    catch(IOException ex){}
  }

  public static void HydraTask_startCluster() {
    Log.getLogWriter().info("Inside HydraTask_startCluster");
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      File log = new File(".");
      String dest = log.getCanonicalPath() + File.separator + "clusterStart.log";
      File logFile = new File(dest);
      snappyCDCTest.startCluster(snappyPath,logFile);
    }
    catch(IOException ex){}
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

  public static void HydraTask_runIngestionApp() {
    Log.getLogWriter().info("Inside HydraTask_runIngestionApp");
    if (snappyCDCTest == null) {
      snappyCDCTest = new SnappyCDCTest();
    }
    snappyCDCTest.runIngestionApp();
  }

  public void runIngestionApp(){
    try {
      Log.getLogWriter().info("Inside runIngestionApp");
      CDCIngestionApp app = new CDCIngestionApp();
      Integer threadCnt = SnappyCDCPrms.getThreadCnt();
      Integer sRange = SnappyCDCPrms.getInitStartRange();
      Integer eRange = SnappyCDCPrms.getInitEndRange();
      String queryPath = SnappyCDCPrms.getDataLocation();
      String sqlServer = SnappyCDCPrms.getSqlServerInstance();
      List<String> endpoints = validateLocatorEndpointData();
      app.runIngestionApp(threadCnt, sRange, eRange, queryPath, sqlServer, endpoints.get(0));
    }
    catch(Exception ex) {
      Log.getLogWriter().info("Caught Exception" + ex.getMessage() + " in runIngestionApp() method");
    }
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

  public static void performHA() {
    String scriptName;
    try {
      String snappyPath = SnappyCDCPrms.getSnappyFileLoc();
      Log.getLogWriter().info("snappyPath File path is " + snappyPath);
      String nodeType = SnappyCDCPrms.getNodeType();
      Boolean isOnlyStop = SnappyCDCPrms.getIsOnlyStop();
      String nodeInfoforHA = SnappyCDCPrms.getNodeConfig();

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
      }
      else {
        if(nodeType.equalsIgnoreCase("servers"))
          scriptName = "/sbin/snappy-servers.sh";
        else if(nodeType.equalsIgnoreCase("leads"))
          scriptName = "/sbin/snappy-leads.sh";
        else
          scriptName = "/sbin/snappy-locators.sh";

        File orgName = new File(snappyPath + "/conf/" + nodeType);
        File bkName = new File(snappyPath + "/conf/" + nodeType + "_bk");

        //rename original conf file
        if (orgName.renameTo(bkName)) {
          Log.getLogWriter().info("File renamed to " + bkName);
        } else {
          Log.getLogWriter().info("Error");
        }

        //write a new file in conf
        try {
          File tempConfFile = new File(snappyPath + "/conf/" + nodeType);
          FileWriter fw = new FileWriter(tempConfFile);
          fw.write(nodeInfoforHA);
          fw.close();
          File log = new File(".");
          String dest = log.getCanonicalPath() + File.separator + "snappyServerSystem.log";
          Log.getLogWriter().info("The destination file is " + dest);
          File logFile = new File(dest);

          if (isOnlyStop)
            stopIndividualNode(snappyPath, scriptName, logFile);
          else {
            Log.getLogWriter().info("The nodeType is " + nodeType + " script to stop is " + scriptName);
            stopIndividualNode(snappyPath, scriptName, logFile);

            Thread.sleep(30000); //sleep for 3 min before restarting the node.

            Log.getLogWriter().info("The nodeType is " + nodeType + " script to start is " + scriptName);
            startIndividualNode(snappyPath, scriptName, logFile);
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
          }
        } catch (FileNotFoundException e) {
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

  public static void removeDiskStore(){
    String dirPath = SnappyCDCPrms.getDataLocation();
    Log.getLogWriter().info("the dirPath is " + dirPath);
    removeDiskStoreFiles(dirPath);
   }

   public static void stopCluster(String snappyPath,File logFile){
     ProcessBuilder pbClustStop = new ProcessBuilder(snappyPath + "/sbin/snappy-stop-all.sh");
     Long startTime = System.currentTimeMillis();
     snappyTest.executeProcess(pbClustStop, logFile);
     Long totalTime = (System.currentTimeMillis() - startTime);
     Log.getLogWriter().info("The cluster took " + totalTime + " ms to shut down");
   }

  public static void startCluster(String snappyPath,File logFile){
    ProcessBuilder pbClustStart = new ProcessBuilder(snappyPath + "/sbin/snappy-start-all.sh");
    Long startTime1 = System.currentTimeMillis();
    snappyTest.executeProcess(pbClustStart, logFile);
    Long totalTime1 = (System.currentTimeMillis() - startTime1);
    Log.getLogWriter().info("The cluster took " + totalTime1 + " ms to shut down");
  }

  public static void stopIndividualNode(String snappyPath,String script,File logFile){
    ProcessBuilder pbStop = new ProcessBuilder(snappyPath + script, "stop");
    snappyTest.executeProcess(pbStop, logFile);
  }

  public static void startIndividualNode(String snappyPath,String script,File logFile){
    ProcessBuilder pbStart = new ProcessBuilder(snappyPath + script, "start");
    snappyTest.executeProcess(pbStart, logFile);
  }

  public static void removeDiskStoreFiles(String dirPath) {
     try {
      File dir = new File(dirPath);
      String[] extensions = new String[] {"crf","drf","krf","idxkrf","if"};
      Log.getLogWriter().info("Getting files with specified extension " + dir.getCanonicalPath());
      List<File> files = (List<File>) FileUtils.listFiles(dir, extensions,false);
      Log.getLogWriter().info("The files length is " + files.size());
      for (File file : files) {
        Log.getLogWriter().info("file: " + file.getCanonicalPath());
        String fileName = file.getName();
        if(file.delete())
          Log.getLogWriter().info("The diskstore file deleted is "+ fileName);
        else
          Log.getLogWriter().info("The diskstore file "+ fileName+ " is not deleted");
        }
     }
    catch(IOException io) {
      Log.getLogWriter().info("Caught exception in getFileWithDiffExt() " + io.getMessage());
    }
   }
}
