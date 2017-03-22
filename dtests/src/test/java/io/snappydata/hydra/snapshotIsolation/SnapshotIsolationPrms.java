package io.snappydata.hydra.snapshotIsolation;

import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyPrms;

public class SnapshotIsolationPrms extends SnappyPrms {

  public static Long tablesList;

  public static Long snappyDDLExtn;

  public static Long dmlOperations;

  public static Long dmlTables;

  public static Long dataFileLocation;

  public static Long csvFileNames;

  public static Long selectStmts;

  public static Long csvLocationforLargeData;

  public static Long insertCsvFileNames;

  public static Long updateStmts;

  public static Long deleteStmts;

  public static Long insertStmts;

  public static String[] getTableNames() {
    Long key = tablesList;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }

  public static String getDMLOperations(){
    Long key = dmlOperations;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String getCsvLocationforLargeData(){
    Long key = csvLocationforLargeData;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String[] getInsertCsvFileNames(){
    Long key = insertCsvFileNames;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }

  public static String[] getDMLTables(){
    Long key = dmlTables;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i); //get what tables are in the tests
    }
    return strArr;
  }

  public static String[] getCSVFileNames() {
    Long key = csvFileNames;
    Vector tables = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[tables.size()];
    for (int i = 0; i < tables.size(); i++) {
      strArr[i] = (String)tables.elementAt(i);
    }
    return strArr;
  }

  public static String getDataLocations() {
    Long key = dataFileLocation;
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, null));
  }

  public static String getSelectStmts(){
    Long key = selectStmts;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String getUpdateStmts(){
    Long key = updateStmts;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String[] getInsertStmts(){
    Long key = insertStmts;
    Vector insertStmts = BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[insertStmts.size()];
    for (int i = 0; i < insertStmts.size(); i++) {
      strArr[i] = (String)insertStmts.elementAt(i);
    }
    return strArr;
  }

  public static String getDeleteStmts(){
    Long key = deleteStmts;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String[] getSnappyDDLExtn() {
    Long key = snappyDDLExtn;
    Vector ddlExtn = TestConfig.tasktab().vecAt(key, TestConfig.tab().vecAt(key, new HydraVector()));
    String[] strArr = new String[ddlExtn.size()];
    for (int i = 0; i < ddlExtn.size(); i++) {
      strArr[i] = (String)ddlExtn.elementAt(i);
    }
    return strArr;
  }

  static {
    SnappyPrms.setValues(SnapshotIsolationPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
