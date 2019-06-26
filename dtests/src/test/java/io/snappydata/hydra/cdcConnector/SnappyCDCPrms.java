package io.snappydata.hydra.cdcConnector;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;

import java.util.Vector;

public class SnappyCDCPrms extends SnappyPrms {

  /**
   * script ,file location
   */
  public static Long dataLocation;

  /**
   * Boolean value to determine if conf is to be modified
   */
  public static Long isModifyConf;

  /**
   * Boolean value to determine if the query is a scan type query
   */

  public static Long isScanQuery;


  /**
   * Boolean value to determine if the test does backUp and recovery
   */
  public static Long isbackUpRecovery;


  /**
   * Boolean value to determine if the query is a pointLookUp type query
   */

  public static Long isPointLookUP;

  /**
   * Boolean value to determine if the query is a mixedType query
   */
  public static Long isMixedQuery;

  /**
   * Boolean value to determine if the query is a bulk delete type query
   */
  public static Long isBulkDelete;

  /**
   * Boolean value to determine if an application is a streaming app
   */
  public static Long isCDCStream;

  /**
   * Boolean value to determine if the test is run with CDC enabled
   */
  public static Long isCDC;

  /**
   * Boolean value to determine if result set validation is before restart or after restart
   */
  public static Long isBeforeRestart;

  /**
   * Boolean value to determine if its only individual node stop
   */
  public static Long isOnlyStop;

  /**
   * Boolean value to determine whether to only start the cluster or stop-start the cluster,default is only start
   */
  public static Long isStopStartCluster;


  /**
   * Int value that determines the number of threads used for a particular application
   */

  public static Long threadCnt;


  /**
   * Int value that determines the number of nodes to be stopped
   */
  public static Long numNodesToStop;


  /**
   * Name of individual applications
   */

  public static Long appName;

  /**
   * Name of the sqlserver database to be used
   */
  public static Long dataBaseName;

  /**
   * config file parameters for the node required during HA
   */
  public static Long nodeInfoForHA;

  /**
   * config file parameters for the node required during HA
   */
  public static Long nodeName;

  /**
   * File path that consists of the confs and cluster scripts
   */
  public static Long snappyFileLoc;

  /**
   * Type of nodes(servers,leads,locators) for HA
   */
  public static Long nodeType;


  /**
   * HostName of the node
   */
  public static Long hostName;

  /**
   * Boolean value to determine if the new node be added in the beginning
   */
  public static Long isNewNodeFirst;


  /**
   * startRange is integer value used for giving
   * the starting range from where the ingestion
   * app should start ingestion
   */
  public static Long initStartRange;

  /**
   * endRange is integer value used for giving
   * the end range at which the ingestion app will stop its ingestion
   */
  public static Long initEndRange;

  /**
   * Name of the sqlServer instance to be used
   */
  public static Long sqlServerInstance;

  /**
   * Boolean value to determine whether to keep the original conf file or not
   */
  public static Long isKeepOrgConf;

  /**
   * String value to determine the the file size .
   */
  public static Long fileSize;

  public static Long tableType;

  public static Long numInserts;

  public static int getNumInserts() {
    Long numInsts = numInserts;
    return tasktab().intAt(numInsts, tab().intAt(numInsts, 2000000));
  }

  public static String getTableType() {
    String tableTyp = tasktab().stringAt(tableType,tab().stringAt(tableType,"column"));
    return tableTyp;
  }

  public static String getFileSize() {
    String fileS = tasktab().stringAt(fileSize,tab().stringAt(fileSize,"10G"));
    return fileS;
  }

  public static String getNodeInfoForHA() {
    String nodeInfo = tasktab().stringAt(nodeInfoForHA, tab().stringAt
        (nodeInfoForHA, null));
    if (nodeInfo == null) return "";
    return nodeInfo;
  }

  public static Vector getNodeName() {
    Long key = nodeName;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static String getSnappyFileLoc() {
    String confLoc = tasktab().stringAt(snappyFileLoc, tab().stringAt
        (snappyFileLoc, null));
    if (confLoc == null) return "";
    return confLoc;
  }


  public static String getDataLocation() {
    String dataLoc = tasktab().stringAt(dataLocation, tab().stringAt
        (dataLocation, null));
    if (dataLoc == null) return "";
    return dataLoc;
  }

  public static String getAppName() {
    String name = tasktab().stringAt(appName, tab().stringAt
        (appName, null));
    if (name == null) return "";
    return name;
  }

  public static String getNodeType() {
    String nodeName = tasktab().stringAt(nodeType, tab().stringAt
        (nodeType, null));
    if (nodeName == null) return "";
    return nodeName;
  }

  public static String getHostName() {
    String nodeName = tasktab().stringAt(hostName, tab().stringAt
        (hostName, null));
    if (nodeName == null) return "";
    return nodeName;
  }

  public static String getDataBaseName() {
    String name = tasktab().stringAt(dataBaseName, tab().stringAt
        (dataBaseName, null));
    if (name == null) return "testdatabase";
    return name;
  }


  public static String getSqlServerInstance() {
    String name = tasktab().stringAt(sqlServerInstance, tab().stringAt
        (sqlServerInstance, null));
    if (name == null) return "";
    return name;
  }

  public static boolean getIsBeforeRestart() {
    Long key = isBeforeRestart;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsModifyConf() {
    Long key = isModifyConf;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsKeepOrgConf() {
    Long key = isKeepOrgConf;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsBackUpRecovery() {
    Long key = isbackUpRecovery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsOnlyStop() {
    Long key = isOnlyStop;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsStopStartCluster() {
    Long key = isStopStartCluster;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsScanQuery() {
    Long key = isScanQuery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsBulkDelete() {
    Long key = isBulkDelete;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsNewNodeFirst() {
    Long key = isNewNodeFirst;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsPointLookUP() {
    Long key = isPointLookUP;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsMixedQuery() {
    Long key = isMixedQuery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsCDC() {
    Long key = isCDC;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsCDCStream() {
    Long key = isCDCStream;
    return tasktab().booleanAt(key, false);
  }

  public static int getInitStartRange() {
    Long key = initStartRange;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  public static int getInitEndRange() {
    Long key = initEndRange;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  public static int getNumNodesToStop() {
    Long key = numNodesToStop;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  public static int getThreadCnt() {
    Long key = threadCnt;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  static {
    SnappyPrms.setValues(SnappyCDCPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
