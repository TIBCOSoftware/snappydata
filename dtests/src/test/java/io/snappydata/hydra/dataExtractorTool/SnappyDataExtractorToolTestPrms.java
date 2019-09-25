package io.snappydata.hydra.dataExtractorTool;

import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappyDataExtractorToolTestPrms extends SnappyPrms {

  public static Long schemaName;

  public static Long isStartClusterForCPDE;


  public static String getSchemaName() {
    String schname = tasktab().stringAt(schemaName, tab().stringAt
        (schemaName, null));
    if (schname == null) return "APP";
    return schname;
  }

  public static boolean getIsStartClusterForCPDE() {
    Long key = isStartClusterForCPDE;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  static {
    SnappyPrms.setValues(io.snappydata.hydra.dataExtractorTool.SnappyDataExtractorToolTestPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
