/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata.hydra.testDMLOps;

import java.util.ArrayList;
import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappySchemaPrms extends SnappyPrms {

  public static Long tablesList;

  public static Long dmlTables;

  public static Long createSchemas;

  public static Long createTablesStatements;

  public static Long snappyDDLExtn;

  public static Long dataFileLocation;

  public static Long csvFileNames;

  public static Long csvLocationforLargeData;

  public static Long insertCsvFileNames;

  public static Long dmlOperations;

  public static Long selectStmts;

  public static Long insertStmts;

  public static Long insertStmtsNonDMLTables;

  public static Long updateStmts;

  public static Long afterUpdateSelects;

//  public static Long selectOrderbyClause;

  public static Long deleteStmts;

  public static Long ddlStmts;

  public static Long afterDeleteSelects;

  public static Long testUniqueKeys;

  public static Long insertTables;

  public static Long updateTables;

  public static Long deleteTables;

  public static Long isHATest;

  public static Long largeDataSet;

  /* boolean : For fast data loading, slipt the data files and load them parallely*/
  public static Long loadDataInParts;

  public static Long numPartsForDataFiles;

  public static Long isSingleBucket;

  public static Long putIntoStmts;

  public  static Long afterPutIntoSelects;

  public static Long afterInsertSelects;

  public static Long batchSize;

  public static Long recreateTablesStatements;

  public static Long insertStmtAfterReCreateTable;

  public static Long dropSchemas;

  public static Long useCsv;

  /* Comma seperated names of column with unique values */
  public static Long uniqueColumnName;


  public static Long isAggregate;

  public  static Long aggType;

  public static String[] getTableNames() {
    Long key = tablesList;
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

  public static String[] getCreateSchemas() {
    Long key = createSchemas;
    Vector statements = TestConfig.tab().vecAt(key, new HydraVector());
    String[] strArr = new String[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      strArr[i] = (String)statements.elementAt(i);
    }
    return strArr;
  }

  public static String[] getDropSchemas() {
    Long key = dropSchemas;
    Vector statements = TestConfig.tab().vecAt(key, new HydraVector());
    String[] strArr = new String[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      strArr[i] = (String)statements.elementAt(i);
    }
    return strArr;
  }
  public static String[] getCreateTablesStatements() {
    Long key = createTablesStatements;
    Vector statements = TestConfig.tab().vecAt(key, new HydraVector());
    String[] strArr = new String[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      strArr[i] = (String)statements.elementAt(i);
    }
    return strArr;
  }

  public static String[] getRecreateTablesStatements() {
    Long key = recreateTablesStatements;
    Vector statements = TestConfig.tab().vecAt(key, new HydraVector());
    String[] strArr = new String[statements.size()];
    for (int i = 0; i < statements.size(); i++) {
      strArr[i] = (String)statements.elementAt(i);
    }
    return strArr;
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

  public static String[] getDDLStmts(){
    Long key = ddlStmts;
    Vector ddlStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[ddlStmt.size()];
    for (int i = 0; i < ddlStmt.size(); i++) {
      strArr[i] = (String)ddlStmt.elementAt(i);
    }
    return strArr;
  }

  public static int getBatchSize(){
    Long key = batchSize;
    return BasePrms.tasktab().intAt(key, BasePrms.tab().intAt(key, 1000));
  }

  public static boolean getIsSingleBucket(){
    Long key = isSingleBucket;
    return TestConfig.tasktab().booleanAt(key, TestConfig.tab().booleanAt(key, false));
  }

  public static String[] getSelectStmts(){
    Long key = selectStmts;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getAfterInsertSelectStmts(){
    Long key = afterInsertSelects;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getAfterUpdateSelectStmts(){
    Long key = afterUpdateSelects;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getAfterDeleteSelectStmts(){
    Long key = afterDeleteSelects;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getAfterPutIntoSelectStmts(){
    Long key = afterPutIntoSelects;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

/*
  public static String[] getOrderByClause(){
    Long key = selectOrderbyClause;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }
*/

  public static String getDMLOperations(){
    Long key = dmlOperations;
    return BasePrms.tasktab().stringAt(key, BasePrms.tab().stringAt(key, null));
  }

  public static String[] getUniqueColumnName(){
    Long key = uniqueColumnName;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getInsertStmts(){
    Long key = insertStmts;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getInsertStmtsForNonDMLTables() {
    Long key = insertStmtsNonDMLTables;
    Vector selectStmt = BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = null;
    if (selectStmt != null) {
      strArr = new String[selectStmt.size()];
      for (int i = 0; i < selectStmt.size(); i++)
        strArr[i] = (String) selectStmt.elementAt(i);

    }
    return strArr;
  }

  public static String[] getInsertTables(){
    Long key = insertTables;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static ArrayList<String> getInsertStmtAfterReCreateTable(){
    Long key = insertStmtAfterReCreateTable;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    ArrayList<String> strArr = new ArrayList<String>();
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr.add(((String)selectStmt.elementAt(i)));
    }
    return strArr;
  }

  public static boolean getLoadDataInParts(){
    Long key = loadDataInParts;
    return TestConfig.tasktab().booleanAt(key, TestConfig.tab().booleanAt(key, false));
  }

  public static boolean isAggregate(){
    Long key = isAggregate;
    return TestConfig.tasktab().booleanAt(key, TestConfig.tab().booleanAt(key, false));
  }

  public static String getAggregateType(){
    Long key = aggType;
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, ""));
  }

  public static boolean hasCsvData(){
    Long key = useCsv;
    return TestConfig.tasktab().booleanAt(key, TestConfig.tab().booleanAt(key, true));
  }

  public static int getNumPartsForDataFiles(){
    Long key = numPartsForDataFiles;
    return TestConfig.tasktab().intAt(key, TestConfig.tab().intAt(key, 1));
  }

  public static String[] getUpdateStmts(){
    Long key = updateStmts;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getDeleteStmts(){
    Long key = deleteStmts;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getPutIntoStmts(){
    Long key = putIntoStmts;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getUpdateTables(){
    Long key = updateTables;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static String[] getDeleteTables(){
    Long key = deleteTables;
    Vector selectStmt =  BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
    String[] strArr = new String[selectStmt.size()];
    for (int i = 0; i < selectStmt.size(); i++) {
      strArr[i] = (String)selectStmt.elementAt(i);
    }
    return strArr;
  }

  public static boolean isTestUniqueKeys() {
    Long key = testUniqueKeys;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean isHATest() {
    Long key = isHATest;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  static {
    SnappyPrms.setValues(SnappySchemaPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
