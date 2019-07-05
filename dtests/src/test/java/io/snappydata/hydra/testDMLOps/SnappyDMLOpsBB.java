package io.snappydata.hydra.testDMLOps;

import hydra.blackboard.Blackboard;
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

    import hydra.blackboard.Blackboard;

public class SnappyDMLOpsBB extends Blackboard {
  //Blackboard creation variables
  static String SNAPPY_DML_BB_NAME = "Snappy_DML_Blackboard";
  static String SNAPPY_DML_BB_TYPE = "RMI";

  public static SnappyDMLOpsBB bbInstance = null;

  /**
   * Get the SnappyBB
   */
  public static synchronized SnappyDMLOpsBB getBB() {
    if (bbInstance == null)
      synchronized (SnappyDMLOpsBB.class) {
        if (bbInstance == null)
          bbInstance = new SnappyDMLOpsBB(SNAPPY_DML_BB_NAME, SNAPPY_DML_BB_TYPE);
      }
    return bbInstance;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public SnappyDMLOpsBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public SnappyDMLOpsBB(String name, String type) {
    super(name, type, SnappyDMLOpsBB.class);
  }

  public static int insertCounter;
}
