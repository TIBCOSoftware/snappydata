/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.cluster;

import java.util.ArrayList;
import java.util.List;

import hydra.Log;
import hydra.blackboard.Blackboard;


/**
 * Created by swati on 22/3/16.
 */
public class SnappyBB extends Blackboard {
    //Blackboard creation variables
    static String SNAPPY_BB_NAME = "Snappy_Blackboard";
    static String SNAPPY_BB_TYPE = "RMI";

    public static SnappyBB bbInstance = null;

    /**
     * Get the SnappyBB
     */
    public static synchronized SnappyBB getBB() {
        if (bbInstance == null)
            synchronized (SnappyBB.class) {
                if (bbInstance == null)
                    bbInstance = new SnappyBB(SNAPPY_BB_NAME, SNAPPY_BB_TYPE);
            }
        return bbInstance;
    }

    /**
     * Zero-arg constructor for remote method invocations.
     */
    public SnappyBB() {
    }

    /**
     * Creates a sample blackboard using the specified name and transport type.
     */
    public SnappyBB(String name, String type) {
        super(name, type, SnappyBB.class);
    }

    public static int serversStarted;
    public static int primaryLocatorStarted;
    public static int doneFullResultSetValidation;
    public static int locatorsStarted;
    public static int snappyClusterStarted;
    public static int leadsStarted;
    public static int sparkClusterStarted;
    public static int doneExecution;
    public static int stopStartVms;
    public static int stopStartLeadVms;
    public static int updateCounter;
    public static int insertCounter;
    public static int deleteCounter;
    public static int queryCounter;
    public static int numServers;
    public static int DMLExecuting;
    public static int leadThread;
    public static int PauseDerby;
    public static int BlockOps;
    public static int leadResultsReady;


    public synchronized List<String> getDerbyOps() {
        List<String> derbyOps = null;
        if (SnappyBB.getBB().getSharedMap().containsKey("derbyOps")) {
            derbyOps = (ArrayList<String>)SnappyBB.getBB().getSharedMap().get("derbyOps");
            SnappyBB.getBB().getSharedMap().remove("derbyOps");
        } else
            Log.getLogWriter().info("No Ops to perform in derby from BB");
        return derbyOps;
    }

    public synchronized void writeOpToBB(String stmt) {
        List<String> derbyOps;
        Log.getLogWriter().info("Adding operation for derby in BB : " + stmt);
        if (SnappyBB.getBB().getSharedMap().containsKey("derbyOps")) {
            derbyOps = (ArrayList<String>)SnappyBB.getBB().getSharedMap().get("derbyOps");
            if(derbyOps==null)
                derbyOps = new ArrayList<String>();
        }  else
            derbyOps = new ArrayList<String>();
        derbyOps.add(stmt);
        SnappyBB.getBB().getSharedMap().put("derbyOps", derbyOps);
        Log.getLogWriter().info("Added operation for derby in BB : " + stmt);
    }
}
