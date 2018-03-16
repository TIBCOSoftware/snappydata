package io.snappydata.hydra.cdcConnector;

import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappyCDCPrms extends SnappyPrms {

  public static Long startRange;

  public static Long endRange;

  public static Long dataLocation;

  public static Long isScanQuery;

  public static Long threadCnt;

  public static Long queryFilePath;

  public static String getDataLocation(){
    String dataLoc = tasktab().stringAt(dataLocation, tab().stringAt
        (dataLocation, null));
    if (dataLoc == null) return "";
    return dataLoc;
  }

  public static boolean getIsScanQuery() {
    Long key = isScanQuery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
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
