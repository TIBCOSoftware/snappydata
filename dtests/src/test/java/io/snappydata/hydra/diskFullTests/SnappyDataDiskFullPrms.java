package io.snappydata.hydra.diskFullTests;

import io.snappydata.hydra.cluster.SnappyPrms;


public class SnappyDataDiskFullPrms extends SnappyPrms {

  /**
   * String value to determine the file size .
   */
  public static Long fileSize;

  /**
   * String value to determine the table type.
   */
  public static Long tableType;

  /**
   * String value to determine no. of inserts .
   */
  public static Long numInserts;

  public static int getNumInserts() {
    Long numInsts = numInserts;
    return tasktab().intAt(numInsts, tab().intAt(numInsts, 2000000));
  }

  public static String getTableType() {
    String tableTyp = tasktab().stringAt(tableType, tab().stringAt(tableType, "column"));
    return tableTyp;
  }

  public static String getFileSize() {
    String fileS = tasktab().stringAt(fileSize, tab().stringAt(fileSize, "10G"));
    return fileS;
  }


  static {
    SnappyPrms.setValues(SnappyDataDiskFullPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
