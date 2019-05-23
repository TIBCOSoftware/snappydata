/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.security;

import hydra.Log;
import hydra.RemoteTestModule;
import io.snappydata.hydra.cdcConnector.SnappyCDCPrms;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import javolution.io.Struct;
import util.TestException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.io.*;
import java.sql.*;


public class SnappySecurityTest extends SnappyTest {

  private static Integer expectedExceptionCnt = 0;
  private static Integer unExpectedExceptionCnt = 0;
  public static Boolean isAuthorized = false;
  public static String adminUser = "gemfire1";
  public static String unAuthUser = "gemfire5";
  public static SnappySecurityTest snappySecurityTest;
  public static String secureBootProp = "";
  private static HashMap<Map<String, ResultSet>, String> policySelectQueryMap = new HashMap<>();
  private static HashMap<Map<String, ResultSet>, String> policyFullSelectQueryMap = new HashMap<>();
  private static HashMap<String, String> policyUserMap = new HashMap<>();


  public static void HydraTask_runQuery() throws SQLException {
    Boolean isRLS = SnappySecurityPrms.getIsRLS();
    if (isRLS)
      validateQuery();
    else    
      runQuery();
  }

  public static void HydraTask_startLdapServer() {
    if (snappySecurityTest == null) {
      snappySecurityTest = new SnappySecurityTest();
    }
    snappySecurityTest.startLdapServer();
  }

  public void startLdapServer() {
    String ldapScriptPath = SnappyCDCPrms.getDataLocation();
    if (snappyTest == null) {
      snappyTest = new SnappyTest();
    }
    try {
      String dest = getCurrentDirPath() + File.separator + "ldapServerStart.log";
//      String propFile = getCurrentDirPath() + File.separator + "../../../secureBootProp.log";
      String propFile = "/export/shared/QA_DATA/secureBootProp.log";
      File ldapServerFile = new File(dest);
      File secureBootPropFile = new File(propFile);
      if(secureBootPropFile.exists())
      {
        secureBootPropFile.delete();
        secureBootPropFile.createNewFile(); 
      }
      if (!ldapServerFile.exists()) {
        String cmd = "nohup " + ldapScriptPath + "/start-ldap-server.sh " + ldapScriptPath + "/auth.ldif > " + ldapServerFile + " & ";
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", cmd);
        snappyTest.executeProcess(pb, ldapServerFile);
      }
      Thread.sleep(30000); //wait until the ldapserver starts.

      String cmd1 = "grep Boot " + dest;
      ProcessBuilder pb1 = new ProcessBuilder("/bin/bash", "-c", cmd1);
      snappyTest.executeProcess(pb1, secureBootPropFile);
     }
     catch (InterruptedException ex) {
      Log.getLogWriter().info("Caught InterruptedException in startLdapServer method " + ex.getMessage());
    }
    catch (IOException io) {
      Log.getLogWriter().info("Caught IO exception in startLdapServer method " + io.getMessage());
    }
  }

  public static String getSecureBootProp() {
    try {
      File log = new File(".");
//      String propFile = log.getCanonicalPath() + File.separator + "../../../secureBootProp.log";
      String propFile = "/export/shared/QA_DATA/secureBootProp.log";
      File secureBootPropFile = new File(propFile);
      FileInputStream fis = new FileInputStream(secureBootPropFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      String str;
      while ((str = br.readLine()) != null) {
        secureBootProp = str.split(" = ")[1];
      }
    }
    catch (IOException io){
      Log.getLogWriter().info("Caught IO exception in startLdapServer method " + io.getMessage());
    }
    Log.getLogWriter().info("The secureBootProps in getSecureBootProp method are " + secureBootProp);
    return secureBootProp;
  }

  public static void HydraTask_getClientConnection() throws SQLException {
    Connection conn = null;
    Vector userVector = SnappySecurityPrms.getUserName();
    Vector passVector = SnappySecurityPrms.getPassWord();
    String user = userVector.elementAt(0).toString();
    String pass = passVector.elementAt(0).toString();
    conn = getSecuredLocatorConnection(user, pass);
    String queryStr1 = "CREATE TABLE " + user + "Table(r1 Integer, r2 Integer) USING COLUMN";
    String queryStr2 = "insert into " + user + "Table VALUES(1,1)";
    String queryStr3 = "DROP TABLE IF EXISTS  " + user + "Table";

    conn.createStatement().execute(queryStr3);
    Log.getLogWriter().info("Dropped " + user+"Table successfully");
    conn.createStatement().execute(queryStr1);
    Log.getLogWriter().info("Created " + user+"Table successfully");
    conn.createStatement().executeUpdate(queryStr2);
    Log.getLogWriter().info("Inserted into " + user+"Table successfully");
    verifyTableData(conn,user);
    closeConnection(conn);
  }

  public static void verifyTableData(Connection conn,String user) throws SQLException {
    String queryStr = "SELECT count(*) from  " + user + "Table";
    ResultSet rs = conn.createStatement().executeQuery(queryStr);
    while (rs.next()) {
      Log.getLogWriter().info("Query executed successfully on table " +user+ "Table and query result is ::" + rs.getInt(1));
    }
  }

  public static void grantSchemaPermisson(Boolean isGrant){
    Vector userVector = SnappySecurityPrms.getUserName();
    String user = userVector.elementAt(0).toString();
    String query= " ";
    String msg = "";
    Connection conn = null;
    if (isGrant) {
      query = "CREATE SCHEMA " + user + " AUTHORIZATION " + user;
      msg = "Create scheam query is  ";
    }
    Log.getLogWriter().info(msg + query);
    try {
      conn = getSecuredLocatorConnection("gemfire1", "gemfire1");
      conn.createStatement().execute(query);
    } catch (SQLException e) {
      Log.getLogWriter().info(" Caught Exception " + e.getMessage());
    }
  }

  public static void grantRevokeOps(Boolean isGrant, Boolean isRevoke, Boolean isPublic) {
    Vector userVector = SnappySecurityPrms.getUserName();
    Vector onSchema = SnappySecurityPrms.getSchema();
    Vector dmlOps = SnappySecurityPrms.getDmlOps();
    String query = " ";
    String msg = "";
    Connection conn = null;
    if (isPublic) {
      for (int s = 0; s < onSchema.size(); s++) {
        for (int o = 0; o < dmlOps.size(); o++) {
          if (isGrant) {
            query = "GRANT " + dmlOps.elementAt(o) + " on " + onSchema.elementAt(s) + " TO  PUBLIC";//grantQuery;
            msg = "The GRANT Query is ";
          }
          if (isRevoke) {
            query = "REVOKE " + dmlOps.elementAt(o) + " on " + onSchema.elementAt(s) + " FROM  PUBLIC";//revokeQuery;
            msg = "The REVOKE query is ";
          }
          Log.getLogWriter().info(msg + query);
          try {
            conn = getSecuredLocatorConnection("gemfire1", "gemfire1");
            conn.createStatement().execute(query);
          } catch (SQLException e) {
          }
        }
      }

    } else {
      for (int i = 0; i < userVector.size(); i++) {
        String user = userVector.elementAt(i).toString(); //entry.getKey();
        for (int s = 0; s < onSchema.size(); s++) {
          for (int o = 0; o < dmlOps.size(); o++) {
            if (isGrant) {
              query = "GRANT " + dmlOps.elementAt(o) + " on " + onSchema.elementAt(s) + " TO ";//grantQuery;
              msg = "The GRANT Query is ";
            }
            if (isRevoke) {
              query = "REVOKE " + dmlOps.elementAt(o) + " on " + onSchema.elementAt(s) + " FROM ";//revokeQuery;
              msg = "The REVOKE query is ";
            }
            String priviligedQ = query + user;
            Log.getLogWriter().info(msg + priviligedQ);
            try {
              conn = getSecuredLocatorConnection("gemfire1", "gemfire1");
              conn.createStatement().execute(priviligedQ);
            } catch (SQLException e) {
            }
          }
        }

      }
    }
    closeConnection(conn);
  }

  public static void HydraTask_performGrantRevokeCmd() {
    Boolean isGrant = SnappySecurityPrms.getIsGrant();
    Boolean isRevoke = SnappySecurityPrms.getIsRevoke();
    Boolean isPublic = SnappySecurityPrms.getIsPublic();
    grantRevokeOps(isGrant, isRevoke, isPublic);
  }

  public static void HydraTask_GrantSchemaPermisson() {
    Boolean isGrant = SnappySecurityPrms.getIsGrant();
    grantSchemaPermisson(isGrant);
  }
  
 public static void HydraTask_enableRLS() {
   //alter table salary enable row level security;
    Log.getLogWriter().info("Inside enableRLS() method ");
    Connection conn = null;
    Boolean isAltrTableRLS = SnappySecurityPrms.getIsAltTableRLS();
    Vector userVector = SnappySecurityPrms.getUserName();
    Vector onSchema = SnappySecurityPrms.getSchema();
    String tableOwner = (String)userVector.elementAt(0);
    try {
      for (int s = 0; s < onSchema.size(); s++) {
        conn = getSecuredLocatorConnection(tableOwner, tableOwner);
        String schemaOwnerTab = (String)onSchema.elementAt(s);
        String[] userTable = schemaOwnerTab.split("\\.");
        if (isAltrTableRLS) {
          String alterTabSql = "ALTER TABLE " + userTable[1] + " ENABLE ROW LEVEL SECURITY";
          Log.getLogWriter().info("The alter table sql is " + alterTabSql);
          conn.createStatement().execute(alterTabSql);
        } else {
          String alterTabSql = "ALTER TABLE " + userTable[1] + " DISABLE ROW LEVEL SECURITY";
          Log.getLogWriter().info("The alter table sql is " + alterTabSql);
          conn.createStatement().execute(alterTabSql);
        }

      }

    } catch (Exception ex) {
      throw new TestException("Caught Exception in executing the enableRLS method " + ex.getMessage());
    }
    Log.getLogWriter().info("enableRLS() method finished successfully ");
  }

  public static void dropPolicy() {
    Log.getLogWriter().info("Inside dropPolicy() ");
    Connection conn = null;
    try {
      HashMap<String, String> tempPolicyMap = policyUserMap;
      for (Map.Entry<String, String> itrr : tempPolicyMap.entrySet()) {
        String policyNm = itrr.getKey();
        String schemaOwner = itrr.getValue();
        try {
          conn = getSecuredLocatorConnection(schemaOwner, schemaOwner);
        } catch (Exception e) {
          Log.getLogWriter().info("The exception is " + e.getMessage());
        }
        Log.getLogWriter().info("Dropping policy p " + policyNm + " with schemaOwner " + schemaOwner);
        conn.createStatement().execute("DROP POLICY " + policyNm);
      }
    } catch (Exception ex) {
      throw new TestException("Caught Exception in executing the dropPolicy()" + ex.getMessage());
    }
    Log.getLogWriter().info("dropPolicy() method finished successfully ");
  }

  public static void createPolicy() {
    Log.getLogWriter().info("Inside createPolicy() ");
    Vector userVector = SnappySecurityPrms.getUserName();
    Vector onSchema = SnappySecurityPrms.getSchema();
    Vector dmlOps = SnappySecurityPrms.getDmlOps();
    int policyCnt = SnappySecurityPrms.getPolicyCnt();
    String filterCond, orderBy;
    Connection conn;
    try {
        //create policy p2 on salary1 for select to user1 using name='a1';
     int cnt = 1;
      for (int i = 0; i < userVector.size(); i++) {
        String policyUser = userVector.elementAt(i).toString();
        for (int p = 0; p < policyCnt; p++) {
          for (int s = 0; s < onSchema.size(); s++) {
            String schemaOwnerTab = (String)onSchema.elementAt(s);
            Log.getLogWriter().info("The schemaOwnerTab is " + schemaOwnerTab + " and size  is " + onSchema.size());
            String[] schemaOwner = schemaOwnerTab.split("\\.");
            Log.getLogWriter().info("The schemaOwner is " + schemaOwner[0]);
            if (schemaOwner[1].equalsIgnoreCase("employees")) {
              filterCond = "EMPLOYEEID = 1 AND COUNTRY = 'USA'";
              orderBy = " ORDER BY EMPLOYEEID asc ";
            } else {
              filterCond = "CATEGORYID = 4";
              orderBy = " ORDER BY CATEGORYID asc ";
            }

            String policyName = "p" + p;
            while (policyUserMap.containsKey(policyName)) {
              Log.getLogWriter().info("checking for policy name " + policyName);
              policyName = "p" + cnt;
              Log.getLogWriter().info("The next policy name is  " + policyName);
              cnt++;
            }
            String policyStr = "CREATE POLICY " + policyName + " ON " + schemaOwnerTab + " FOR " + dmlOps.elementAt(0) + " TO " + policyUser + " USING " + filterCond;
            //Equivalent select query will be :
            String selectQryWF = "SELECT * FROM " + schemaOwnerTab + " WHERE " + filterCond;
            String selectQry = "SELECT * FROM " + schemaOwnerTab + orderBy;
            Map<String, ResultSet> queryResultMap = new HashMap<>();
            Map<String, ResultSet> queryResultMap1 = new HashMap<>();
            Log.getLogWriter().info("Policy created for " + policyUser + " on table " + onSchema.elementAt(s) + " is " + policyStr);
            try {
              conn = getSecuredLocatorConnection(schemaOwner[0], schemaOwner[0]);
              conn.createStatement().execute(policyStr);
              ResultSet rs = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(selectQryWF);//select query with filter conditions
              ResultSet rs1 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(selectQry);//select query with no filter conditions
              //Hash map consisting of <policyQry,equivalentSelectQry>,user
              queryResultMap.put(selectQryWF, rs);
              queryResultMap1.put(selectQry, rs1);
              policySelectQueryMap.put(queryResultMap, policyUser);
              policyUserMap.put(policyName, schemaOwner[0]);
              policyFullSelectQueryMap.put(queryResultMap1, policyUser);
            } catch (SQLException e) {
              throw new TestException("Caught Exception in executing the policy sql" + e.getMessage() + " stackTrace = " + e.getCause());
            }
          }
        }
      }
    } catch (Exception ex) {
      throw new TestException("Caught Exception in createPolicy() " + ex.getMessage());
    }
    Log.getLogWriter().info("createPolicy() method finished successfully ");
  }

public static void validateQuery() {
    Log.getLogWriter().info("Inside validateQuery() ");
    Connection conn = null;
    Boolean isDropPolicy = SnappySecurityPrms.getIsDropPolicy();
    Boolean isAltTableRLS = SnappySecurityPrms.getIsAltTableRLS();
    HashMap<Map<String, ResultSet>, String> queryUserMap;
    if (isDropPolicy || !isAltTableRLS) {
      queryUserMap = policyFullSelectQueryMap; // this map contains rs fro select * from table query
      Log.getLogWriter().info("Using policyFullSelectQueryMap");
    } else {
      queryUserMap = policySelectQueryMap; // this map contains rs fro select * from table with condition query
      Log.getLogWriter().info("Using policySelectQueryMap");
    }
    ResultSetMetaData currRSMD;
    ResultSetMetaData prevRSMD;
    ResultSet currRS;
    ResultSet currFiltrRS;
    try {
      for (Map.Entry<Map<String, ResultSet>, String> entry : queryUserMap.entrySet()) {
        Map<String, ResultSet> queryMap = entry.getKey();
        String policyUser = entry.getValue();
        Log.getLogWriter().info("The user is " + policyUser);
        for (Map.Entry<String, ResultSet> itrr : queryMap.entrySet()) {
          String selectQry = itrr.getKey();
          String[] finalSelectQ = selectQry.split("WHERE");
          ResultSet prevRS = itrr.getValue();
          Log.getLogWriter().info("The select Query is " + selectQry);
          Log.getLogWriter().info("The Final select Query tobe executed by policy User is  " + finalSelectQ[0]);
          conn = getSecuredLocatorConnection(policyUser, policyUser);
          try {
            currRS = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(finalSelectQ[0]);
            currFiltrRS = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(selectQry);
            int currRSSize = getRSCount(currRS);
            int prevRSSize = getRSCount(prevRS);
            int currFiltrRSSize = getRSCount(currFiltrRS);
            Log.getLogWriter().info("The currRSSize = " + currRSSize + " and prevRSSize = " + prevRSSize + " and  currFiltrRSSize = " + currFiltrRSSize);
            //Compare the prevRS and rs ,it should be same.
            if ((currRSSize == prevRSSize) && (currFiltrRSSize == prevRSSize)) {
              while (currRS.next() && prevRS.next()) {
                currRSMD = currRS.getMetaData();
                prevRSMD = prevRS.getMetaData();
                int currRSColCnt = currRSMD.getColumnCount();
                int prevRSColCnt = prevRSMD.getColumnCount();
                Log.getLogWriter().info("The currRSColCnt count is " + currRSColCnt + " \n The prevRSColCnt col cnt is " + prevRSColCnt);
                if (currRSColCnt == prevRSColCnt) {
                  for (int i = 1; i <= currRSColCnt; i++) {
                    Object currRowVal = currRS.getObject(i);
                    Object prevRowVal = prevRS.getObject(i);
                    if (currRowVal != null && prevRowVal != null)
                      Log.getLogWriter().info(" CLass of object is = " + currRowVal.getClass() + " \n Converted to string is " + currRowVal.toString() + " \n Policy user row val = " + prevRowVal.toString());
                   //Compare the row vals it should be same.
                  }
                } else {
                  Log.getLogWriter().info("ERROR : The col cnts donot match ");
                }
              }
              prevRS.beforeFirst();
            } else
              Log.getLogWriter().info("Caught exception : currRSCnt = " + currRSSize + " is not equal to  prevRSSize = " + prevRSSize + " and not equal to currFiltrRSSize = " + currFiltrRSSize);
          } catch (SQLException ex) {
            Log.getLogWriter().info("Got sql exception " + ex.getMessage());
          }
        }
      }
    } catch (Exception ex) {
      throw new TestException("Caught Exception in validateQuery " + ex.getMessage());
    }
    Log.getLogWriter().info("validateQuery() method finished successfully ");
  }

 
  public static int getRSCount(ResultSet resultSet) {
    int size = 0;
    try {
      resultSet.last();
      size = resultSet.getRow();
      resultSet.beforeFirst();
    } catch (Exception ex) {
      Log.getLogWriter().info("Caught exception " + ex.getMessage());
      return 0;
    }
    return size;
  }

  public static void HydraTask_createPolicy() {
    createPolicy();
  }

  public static void HydraTask_dropPolicy() {
    dropPolicy();
  }

                    
  public static ArrayList getQueryArr(String fileName, String user) {
    Log.getLogWriter().info("Inide getQueryArray");
    Log.getLogWriter().info("User = " + user);
    Log.getLogWriter().info("File Name = " + fileName);
    ArrayList<String> queries = new ArrayList<String>();
    Vector schemaToTest = SnappySecurityPrms.getSchema();
    String str = schemaToTest.elementAt(0).toString();
    String schemaOwner = str.split("\\.")[0];
    Log.getLogWriter().info("The schema owner is " + schemaOwner);
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line = null;
      Log.getLogWriter().info("SS");
      while ((line = br.readLine()) != null) {
        String[] splitData = line.split(";");
        Log.getLogWriter().info("splitData length is " + splitData.length);
        for (int i = 0; i < splitData.length; i++) {
            if (!(splitData[i] == null) || !(splitData[i].length() == 0)) {
            String qry = splitData[i].replace("gemfire2", schemaOwner);
            queries.add(qry);
          }
        }
      }
      br.close();
    } catch (FileNotFoundException e) {
    } catch (IOException io) {
    }
    return queries;
  }

  public static void switchCase(String user, String passwd) throws SQLException {
    Boolean isGrant = SnappySecurityPrms.getIsGrant();
    Boolean isPublic = SnappySecurityPrms.getIsPublic();
    switch (user) {
      case "gemfire1":
        runQuery(user, passwd, true);
        break;
      case "gemfire2":
        runQuery(user, passwd, true);
        break;
      case "gemfire3":
        if (isGrant)
          isAuthorized = true;
        else
          isAuthorized = false;
        runQuery(user, passwd, isAuthorized);
        break;
      case "gemfire4":
        if (isGrant)
          isAuthorized = true;
        else
          isAuthorized = false;
        runQuery(user, passwd, isAuthorized);
        break;
      case "gemfire5" :
        if(isGrant && isPublic)
          isAuthorized = true;
        else
          isAuthorized = false;
        runQuery(user, passwd, isAuthorized);
        break;
    }
  }

  public static void runQuery() throws SQLException {
    Log.getLogWriter().info("Inside runQuery without args");
    Vector userVector = SnappySecurityPrms.getUserName();
    Vector passVector = SnappySecurityPrms.getPassWord();
    int expectedExcptCnt = SnappySecurityPrms.getExpectedExcptCnt();
    int unExpectedExcptCnt = SnappySecurityPrms.getUnExpectedExcptCnt();
    for (int i = 0; i < userVector.size(); i++) {
      String user = userVector.elementAt(i).toString();
      String passwd = passVector.elementAt(i).toString();
      try {
        switchCase(user, passwd);
      } catch (Exception e) {
        throw new TestException("Caught Exception " + e.getMessage());
      }

    }
   validate(expectedExcptCnt, unExpectedExcptCnt);
  }

  public static void runQuery(String usr, String pass, Boolean isAuth) throws SQLException {
    Log.getLogWriter().info("Inside runQuery with args ");
    Connection conn = null;
    Vector schemaToTest = SnappySecurityPrms.getSchema();
    Log.getLogWriter().info("schemaToTest " + schemaToTest.size());
    String schemaStr = schemaToTest.elementAt(0).toString();
    Log.getLogWriter().info("schemaStr = " + schemaStr);
    String schemaOwner = schemaStr.split("\\.")[0];
    Log.getLogWriter().info(" SchemaOwner is " + schemaOwner);
    Boolean isGrant = SnappySecurityPrms.getIsGrant();
    Log.getLogWriter().info("User = " + usr + " with passwd = " + pass + " with  authorization = " + isAuth + " and grant permisison is " + isGrant + "with schemaOwner = " + schemaOwner + "  will execute the below query \n");
    conn = getSecuredLocatorConnection(usr, pass);
    String fileName = SnappySecurityPrms.getDataLocation();
    ArrayList queryArray = getQueryArr(fileName, usr);
    Boolean isSelect = true;
    Vector dmlOps = SnappySecurityPrms.getDmlOps();
    if (!(dmlOps.contains("SELECT")))
      isSelect = false;
    for (int q = 0; q < queryArray.size(); q++) {
      String queryStr = (String)queryArray.get(q);
      Boolean opAuth = false;
      Boolean schemaAuth = false;
      try {
        if (!usr.equals(adminUser) && !usr.equals(schemaOwner) && isGrant) {
          for (int d = 0; d < dmlOps.size(); d++) {
            String dmlOp = dmlOps.elementAt(d).toString();
            Log.getLogWriter().info("Find " + dmlOp + " in query " + queryStr);
            if (queryStr.contains(dmlOp)) {
              if (!isSelect) {
                if (dmlOp.equals("INSERT"))
                  opAuth = true;
              } else
                opAuth = true;
            }
          }
          for (int s = 0; s < schemaToTest.size(); s++) {
            String str = schemaToTest.elementAt(s).toString();
            Log.getLogWriter().info("Find " + str + " in query " + queryStr);
            if (queryStr.contains(str))
              schemaAuth = true;
          }
          if ((!opAuth || !schemaAuth) || (!opAuth && !schemaAuth)) {
            isAuth = false;
            Log.getLogWriter().info("The user " + usr + "will execute the query   " + queryStr + " with new authorization = " + isAuth);
            execute(queryStr, conn);
          } else {
            if (usr.equals(unAuthUser))
              isAuth = false;
            else
              isAuth = true;
            Log.getLogWriter().info("The user " + usr + "will execute the query   " + queryStr + " with new authorization = " + isAuth);
            execute(queryStr, conn);
          }
          //   }
             /*for (int s = 0; s < schemaToTest.size(); s++) {
              String str = schemaToTest.elementAt(s).toString();
              Log.getLogWriter().info("Find " + str + " in query " + queryStr);
              if (!queryStr.contains(str)) {
                isAuth = false;
                Log.getLogWriter().info("The user " + usr + "will execute the query   " + queryStr + " with new authorization = " + isAuth);
                 execute(queryStr, conn);
              } else {
                if (usr.equals(unAuthUser))
                  isAuth = false;
                else
                  isAuth = true;
                Log.getLogWriter().info("The user " + usr + "will execute the query   " + queryStr + " with new authorization = " + isAuth);
                execute(queryStr, conn);
              }
            }*/

        } else {
          Log.getLogWriter().info("The query to be executed is  " + queryStr + " with new authorization = " + isAuth);
          execute(queryStr, conn);
        }
      } catch (SQLException e) {
        if (e.toString().contains("SQLState=425")) {
          if (isAuth) {
            unExpectedExceptionCnt = unExpectedExceptionCnt + 1;
            Log.getLogWriter().info(" unExpectedExceptionCnt Count is " + unExpectedExceptionCnt);
            Log.getLogWriter().info("Got UnExpected Exception " + e.getMessage());
          } else {
            expectedExceptionCnt = expectedExceptionCnt + 1;
            Log.getLogWriter().info("Got Expected Exception " + e.getMessage());
          }
        } else {
          Log.getLogWriter().info("CAUGHT EXCEPTION : " + e.getMessage());
          // throw e;
        }
      }
    }
    closeConnection(conn);
  }

  public static void execute(String queryStr, Connection conn) throws SQLException {
    if (queryStr.contains("SELECT"))
      conn.createStatement().executeQuery(queryStr);
    else
      conn.createStatement().execute(queryStr);
    Log.getLogWriter().info("Query executed successfully");
  }

  public static void validate(Integer expectedCnt, Integer unExpectedCnt) {
    if (unExpectedExceptionCnt != unExpectedCnt)
      throw new TestException("The Result is WRONG :Expected unExpectedExceptionCnt = " + unExpectedCnt + " but got " +
          unExpectedExceptionCnt);
    else
      Log.getLogWriter().info("Successfully Got expected unExpectedExceptionCnt " + unExpectedExceptionCnt);

    if (expectedExceptionCnt != expectedCnt)
      throw new TestException("The Result is WRONG :Expected expectedExceptionCnt = " + expectedCnt + " but got " +
          expectedExceptionCnt);
    else
      Log.getLogWriter().info("Successfully Got expected expectedExceptionCnt " + expectedExceptionCnt);

    unExpectedExceptionCnt = 0;
    expectedExceptionCnt = 0;
  }

  public static Connection getSecuredLocatorConnection(String usr, String pass) throws SQLException {
    List<String> endpoints = validateLocatorEndpointData();
    Properties props = new Properties();
    props.setProperty("user", usr);
    props.setProperty("password", pass);
    Connection conn = null;
    String url = "jdbc:snappydata://" + endpoints.get(0) + "/";
    conn = getConnection(url, "io.snappydata.jdbc.ClientDriver", props);
    return conn;
  }

  private static Connection getConnection(String protocol, String driver, Properties props)
      throws
      SQLException {
    Log.getLogWriter().info("Creating secure connection using " + driver + " with " + protocol +
        " and credentials = " + props.getProperty("user") + " " + props.getProperty("password"));
    loadDriver(driver);
    Connection conn = DriverManager.getConnection(protocol, props);
    return conn;
  }

  public static synchronized void HydraTask_executeSQLScripts() {
    Vector scriptNames, dataLocationList = null, persistenceModeList = null,
        colocateWithOptionList = null, partitionByOptionList = null, numPartitionsList =
        null, redundancyOptionList = null, recoverDelayOptionList = null,
        maxPartitionSizeList = null, evictionByOptionList = null;
    File log = null, logFile = null;
    scriptNames = SnappyPrms.getSQLScriptNames();
    Vector userVector = SnappySecurityPrms.getUserName();
    Vector passVector = SnappySecurityPrms.getPassWord();
    if (scriptNames == null) {
      String s = "No Script names provided for executing in the Hydra TASK";
      throw new TestException(s);
    }
    try {
      dataLocationList = SnappyPrms.getDataLocationList();
      persistenceModeList = SnappyPrms.getPersistenceModeList();
      colocateWithOptionList = SnappyPrms.getColocateWithOptionList();
      partitionByOptionList = SnappyPrms.getPartitionByOptionList();
      numPartitionsList = SnappyPrms.getNumPartitionsList();
      redundancyOptionList = SnappyPrms.getRedundancyOptionList();
      recoverDelayOptionList = SnappyPrms.getRecoverDelayOptionList();
      maxPartitionSizeList = SnappyPrms.getMaxPartitionSizeList();
      evictionByOptionList = SnappyPrms.getEvictionByOptionList();
      if (dataLocationList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the dataLocationList for the  " +
            "scripts for which no dataLocation is specified.");
        while (dataLocationList.size() != scriptNames.size())
          dataLocationList.add(" ");
      }
      if (persistenceModeList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"sync\" parameter in the persistenceModeList for" +
            "  the scripts for which no persistence mode is specified.");
        while (persistenceModeList.size() != scriptNames.size())
          persistenceModeList.add("sync");
      }
      if (colocateWithOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"none\" parameter in the colocateWithOptionList " +
            " for the scripts for which no COLOCATE_WITH Option is specified.");
        while (colocateWithOptionList.size() != scriptNames.size())
          colocateWithOptionList.add("none");
      }
      if (partitionByOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the partitionByOptionList for " +
            " the scripts for which no PARTITION_BY option is specified.");
        while (partitionByOptionList.size() != scriptNames.size())
          partitionByOptionList.add(" ");
      }
      if (numPartitionsList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"113\" parameter in the partitionByOptionsList " +
            "for  the scripts for which no BUCKETS option is specified.");
        while (numPartitionsList.size() != scriptNames.size())
          numPartitionsList.add("113");
      }
      if (redundancyOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the redundancyOptionList for " +
            "the  scripts for which no REDUNDANCY option is specified.");
        while (redundancyOptionList.size() != scriptNames.size())
          redundancyOptionList.add(" ");
      }
      if (recoverDelayOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the recoverDelayOptionList for" +
            "  the scripts for which no RECOVER_DELAY option is specified.");
        while (recoverDelayOptionList.size() != scriptNames.size())
          recoverDelayOptionList.add(" ");
      }
      if (maxPartitionSizeList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \" \" parameter in the maxPartitionSizeList for " +
            "the  scripts for which no MAX_PART_SIZE option is specified.");
        while (maxPartitionSizeList.size() != scriptNames.size())
          maxPartitionSizeList.add(" ");
      }
      if (evictionByOptionList.size() != scriptNames.size()) {
        Log.getLogWriter().info("Adding \"LRUHEAPPERCENT\" parameter in the  " +
            "evictionByOptionList for the scripts for which no EVICTION_BY option is" +
            " specified.");
        while (evictionByOptionList.size() != scriptNames.size())
          evictionByOptionList.add("LRUHEAPPERCENT");
      }
      for (int i = 0; i < scriptNames.size(); i++) {
        String userScript = (String)scriptNames.elementAt(i);
        String location = (String)dataLocationList.elementAt(i);
        String persistenceMode = (String)persistenceModeList.elementAt(i);
        String colocateWith = (String)colocateWithOptionList.elementAt(i);
        String partitionBy = (String)partitionByOptionList.elementAt(i);
        String numPartitions = (String)numPartitionsList.elementAt(i);
        String redundancy = (String)redundancyOptionList.elementAt(i);
        String recoverDelay = (String)recoverDelayOptionList.elementAt(i);
        String maxPartitionSize = (String)maxPartitionSizeList.elementAt(i);
        String evictionByOption = (String)evictionByOptionList.elementAt(i);
        Log.getLogWriter().info("Location is " + location);
        String dataLocation = snappyTest.getDataLocation(location);
        String filePath = snappyTest.getScriptLocation(userScript);
        log = new File(".");
        String dest = log.getCanonicalPath() + File.separator + "sqlScriptsResult_" +
            RemoteTestModule.getCurrentThread().getThreadId() + ".log";
        logFile = new File(dest);
        String primaryLocatorHost = getPrimaryLocatorHost();
        String primaryLocatorPort = getPrimaryLocatorPort();
        for (int j = 0; j < userVector.size(); j++) {
          String user = userVector.elementAt(j).toString();
          String pass = passVector.elementAt(j).toString();
          ProcessBuilder pb = new ProcessBuilder(SnappyShellPath, "run", "-file=" +
              filePath, "-param:dataLocation=" + dataLocation,
              "-param:persistenceMode=" + persistenceMode, "-param:colocateWith=" +
              colocateWith, "-param:partitionBy=" + partitionBy,
              "-param:numPartitions=" + numPartitions, "-param:redundancy=" +
              redundancy, "-param:recoverDelay=" + recoverDelay,
              "-param:maxPartitionSize=" + maxPartitionSize, "-param:evictionByOption="
              + evictionByOption, "-client-port=" + primaryLocatorPort,
              "-client-bind-address=" + primaryLocatorHost, "-user=" + user, "-password=" + pass);

          Log.getLogWriter().info(" cmd " + pb.command());
          snappyTest.executeProcess(pb, logFile);
        }
      }
    } catch (IOException e) {
      throw new TestException("IOException occurred while retriving destination logFile " +
          "path  " + log + "\nError Message:" + e.getMessage());
    }
  }
}
