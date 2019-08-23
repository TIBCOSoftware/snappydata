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

package io.snappydata.hydra.connectionPool;

import hydra.BasePrms;
import hydra.TestConfig;

public class SnappyConnectionPoolPrms extends BasePrms {

  public static Long useConnPool;

  public static Long url;

  public static Long driver;

  public static Long username;

  public static Long password;

  public static Long initialSize;

  public static Long maxWait;

  public static Long poolName;

  public static String getString(Long key){
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, null));
  }
  public static String getUrl() {
    Long key = url;
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, "jdbc:snappydata://"));
  }

  public static String getDriver() {
    Long key = driver;
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, "io.snappydata.jdbc" +
        ".ClientDriver"));
  }

  public static String getUsername() {
    return getString(username);
  }

  public static String getPassword() {
    return getString(password);
  }

  public static String getPoolName() {
    Long key = poolName;
    return TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, "myConnPool"));
  }

  public static int getInitialSize() {
    Long key = initialSize;
    return TestConfig.tasktab().intAt(key, TestConfig.tab().intAt(key, 100));
  }

  public static int getMaxWait() {
    Long key = maxWait;
    return TestConfig.tasktab().intAt(key, TestConfig.tab().intAt(key, 10000));
  }

  public static int getConnPoolType(String connPoolType) {
    // 0 - HikariConnPool
    // 1 - TomcatConnPool
    // -1 - dont use connection pool
    if (connPoolType.equalsIgnoreCase("Hikari"))
      return 0;
    else if (connPoolType.equalsIgnoreCase("Tomcat"))
      return 1;
    else
      return -1; //don't use connection pool
  }

  static {
    BasePrms.setValues(SnappyConnectionPoolPrms.class);
  }

  public static void main(String args[]) {
    BasePrms.dumpKeys();
  }
}
