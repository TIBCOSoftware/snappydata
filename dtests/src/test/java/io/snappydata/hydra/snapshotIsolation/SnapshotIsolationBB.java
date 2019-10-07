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

package io.snappydata.hydra.snapshotIsolation;

import hydra.blackboard.Blackboard;


public class SnapshotIsolationBB extends Blackboard {
  //Blackboard creation variables
  static String SNAPSHOT_BB_NAME = "Snapshot_Blackboard";
  static String SNAPSHOT_BB_TYPE = "RMI";

  public static SnapshotIsolationBB bbInstance = null;

  /**
   * Get the SnappyBB
   */
  public static synchronized SnapshotIsolationBB getBB() {
    if (bbInstance == null)
      synchronized (SnapshotIsolationBB.class) {
        if (bbInstance == null)
          bbInstance = new SnapshotIsolationBB(SNAPSHOT_BB_NAME, SNAPSHOT_BB_TYPE);
      }
    return bbInstance;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public SnapshotIsolationBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public SnapshotIsolationBB(String name, String type) {
    super(name, type, SnapshotIsolationBB.class);
  }

  public static int DMLExecuting;
  public static int PauseDerby;
  public static int BlockOps;
  public static int firstThread;
  public static int firstResultsReady;
  public static int numRowsInserted;

}
