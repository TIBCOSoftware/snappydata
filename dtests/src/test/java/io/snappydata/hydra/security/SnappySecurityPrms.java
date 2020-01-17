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
package io.snappydata.hydra.security;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyPrms;

import java.util.Vector;


public class SnappySecurityPrms extends SnappyPrms{

  public static Long queryList;

  public static Long userName;

  public static Long passWord;

  public static Long expectedExcptCnt;

  public static Long unExpectedExcptCnt;

  public static Long dataLocation;

  public static Long isGrant;

  public static Long isRevoke;

  public static Long isJoinQuery;

  public static Long isPublicAccess;

  public static Long isGroupAccess;

  public static Long onSchema;

  public static Long dmlOperations;

  public static Long isSecurity;

  public static Vector getDmlOps() {
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

  public static boolean getIsJoinQuery() {
    Long key = isJoinQuery;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }


  public static boolean getIsPublic() {
    Long key = isPublicAccess;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static boolean getIsGroup() {
    Long key = isGroupAccess;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static int getExpectedExcptCnt() {
    Long key = expectedExcptCnt;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  public static int getUnExpectedExcptCnt() {
    Long key = unExpectedExcptCnt;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  public static Vector getUserName() {
    Long key = userName;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

  public static Vector getPassWord() {
    Long key = passWord;
    return BasePrms.tasktab().vecAt(key, BasePrms.tab().vecAt(key, null));
  }

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
