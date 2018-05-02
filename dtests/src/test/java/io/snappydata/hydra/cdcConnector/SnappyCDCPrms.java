package io.snappydata.hydra.cdcConnector;

import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappyCDCPrms extends SnappyPrms {

  /**script ,file location */
  public static Long dataLocation;

  /**Boolean value to determine if the query is a scan type query*/
  public static Long isScanQuery;

  /**Boolean value to determine if the query is a pointLookUp type query*/
  public static Long isPointLookUP;

  /**Boolean value to determine if the query is a mixedType query*/
  public static Long isMixedQuery;

  /**Boolean value to determine if the query is a bulk delete type query*/
  public static Long isBulkDelete;

  /**Boolean value to determine if an application is a streaming app*/
  public static Long isCDCStream;

  /**Boolean value to determine if the test is run with CDC enabled*/
  public static Long isCDC;

  /**Int value that determines the number of threads used for a particular application*/
  public static Long threadCnt;

  /**Name of individual applications*/
  public static Long appName;

  /**Name of the sqlserver database to be used*/
  public static Long dataBaseName;

  /**config file parameters for the node required during HA*/
  public static Long nodeInfoforHA;

  /**File path that consists of the confs and cluster scripts*/
  public static Long snappyFileLoc;

  /**Type of nodes(servers,leads,locators) for HA*/
  public static Long nodeType;

  /**startRange is integer value used for giving
   *  the starting range from where the ingestion
   *  app should start ingestion
   */
  public static Long initStartRange;

  /**endRange is integer value used for giving
   *  the end range at which the ingestion app will stop its ingestion
   */
  public static Long initEndRange;

  /**Name of the sqlServer instance to be used*/
  public static Long sqlServerInstance;

  public static String getNodeInfoforHA(){
    String nodeInfo = tasktab().stringAt(nodeInfoforHA, tab().stringAt
        (nodeInfoforHA, null));
    if (nodeInfo == null) return "";
    return nodeInfo;
  }

  public static String getSnappyFileLoc(){
    String confLoc = tasktab().stringAt(snappyFileLoc, tab().stringAt
        (snappyFileLoc, null));
    if (confLoc == null) return "";
    return confLoc;
  }


  public static String getDataLocation(){
    String dataLoc = tasktab().stringAt(dataLocation, tab().stringAt
        (dataLocation, null));
    if (dataLoc == null) return "";
    return dataLoc;
  }

  public static String getAppName(){
    String name = tasktab().stringAt(appName, tab().stringAt
        (appName, null));
    if (name == null) return "";
    return name;
  }

  public static String getNodeType(){
    String nodeName = tasktab().stringAt(nodeType, tab().stringAt
        (nodeType, null));
    if (nodeName == null) return "";
    return nodeName;
  }

  public static String getDataBaseName(){
    String name = tasktab().stringAt(dataBaseName, tab().stringAt
        (dataBaseName, null));
    if (name == null) return "testdatabase";
    return name;
  }


  public static String getSqlServerInstance(){
    String name = tasktab().stringAt(sqlServerInstance, tab().stringAt
        (sqlServerInstance, null));
    if (name == null) return "sqlServer1";
    return name;
  }

  public static boolean getIsScanQuery() {
    Long key = isScanQuery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsBulkDelete() {
    Long key = isBulkDelete;
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
