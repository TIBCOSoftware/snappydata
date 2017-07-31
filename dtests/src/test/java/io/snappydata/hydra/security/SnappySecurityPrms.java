package io.snappydata.hydra.security;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;

import java.util.Vector;


public class SnappySecurityPrms extends SnappyPrms{

  /**
   * Parameter used to get the user list of pointLookUP queries to execute concurrently using
   * jdbc clients.
   * (VectorsetValues of Strings) A list of values for pointLookUp queries.
   */
  public static Long queryList;

  /**
   * (boolean) for testing security
   */

  public static Long userName;

  public static Long passWord;

  public static Long expectedExcptCnt;

  public static Long unExpectedExcptCnt;

  public static Long dataLocation;

  public static Long isGrant;

  public static Long isRevoke;

  public static Long isPublicAccess;

  public static Long onSchema;

  public static Long dmlOperations;

  public static Vector getDmlOps() {
   // Log.getLogWriter().info("SPInside getDmlOps");
    Long key = dmlOperations;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static String getDataLocation(){
    String dataLoc = tasktab().stringAt(dataLocation, tab().stringAt
        (dataLocation, null));
    if (dataLoc == null) return "";
    return dataLoc;
  }

  public static Vector getSchema() {
   // Log.getLogWriter().info("SPInside getSchema");
    Long key = onSchema;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }
  public static boolean getIsGrant() {
    Long key = isGrant;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsRevoke() {
    Long key = isRevoke;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsPublic() {
    Long key = isPublicAccess;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static int getExpectedExcptCnt() {
    Long key = expectedExcptCnt;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  public static int getUnExpectedExcptCnt() {
    Long key = unExpectedExcptCnt;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }

  public static Vector getUserName() {
   // Log.getLogWriter().info("SPInside getUserName");
    Long key = userName;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static Vector getPassWord() {
  //  Log.getLogWriter().info("SPInside getPassewd");
    Long key = passWord;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  /*public static Vector getSQLScriptNames() {
    Long key = sqlScriptNames;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }*/
  public static Vector getQueryList() {
    Long key = queryList;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, new HydraVector()));
  }

  static {
    SnappyPrms.setValues(SnappySecurityPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
