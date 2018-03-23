package io.snappydata.hydra.cdcConnector;

import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappyCDCPrms extends SnappyPrms {

  /**startRange is integer value used for giving
   *  the starting range from where the ingestion
   *  app should start ingestion
   */
  public static Long startRange;

  /**endRange is integer value used for giving
   *  the end range at which the ingestion app will stop its ingestion
   */
  public static Long endRange;

  public static Long dataLocation;

  public static Long isScanQuery;

  public static Long isCDCStream;

  public static Long isCDC;

  public static Long threadCnt;

  public static Long appName;

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

  public static boolean getIsScanQuery() {
    Long key = isScanQuery;
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

  public static int getStartRange() {
    Long key = startRange;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  public static int getEndRange() {
    Long key = endRange;
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
