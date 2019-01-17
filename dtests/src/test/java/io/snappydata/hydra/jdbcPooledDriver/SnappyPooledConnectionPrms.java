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
package io.snappydata.hydra.jdbcPooledDriver;

import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;
import io.snappydata.hydra.cluster.SnappyPrms;

public class SnappyPooledConnectionPrms extends SnappyPrms {

  public static Long isPooledConnection;

  public static boolean getIsPooledConnection() {
    Long key = isPooledConnection;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  static {
    SnappyPrms.setValues(io.snappydata.hydra.security.SnappySecurityPrms.class);
  }

  public static void main(String args[]) {
    SnappyPrms.dumpKeys();
  }
}
