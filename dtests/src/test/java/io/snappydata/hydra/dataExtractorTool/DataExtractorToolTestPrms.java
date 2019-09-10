package io.snappydata.hydra.dataExtractorTool;

import io.snappydata.hydra.cluster.SnappyPrms;

public class DataExtractorToolTestPrms extends SnappyPrms {

  public static Long schemaName;


  public static String getSchemaName() {
    String schname = tasktab().stringAt(schemaName, tab().stringAt
        (schemaName, null));
    if (schname == null) return "APP";
    return schname;
  }

  static {
    SnappyPrms.setValues(io.snappydata.hydra.cdcConnector.SnappyCDCPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
