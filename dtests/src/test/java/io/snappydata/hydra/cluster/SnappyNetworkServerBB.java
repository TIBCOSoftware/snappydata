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
package io.snappydata.hydra.cluster;

import hydra.blackboard.Blackboard;

/**
 * Holds ports for snappy network servers. e.g. server, locator
 */
public class SnappyNetworkServerBB extends Blackboard {

    private static SnappyNetworkServerBB blackboard;

    public static int connectionCount;

    /**
     * Zero-arg constructor for remote method invocations.
     */
    public SnappyNetworkServerBB() {
    }

    /**
     * Creates a blackboard using the specified name and transport type.
     */
    public SnappyNetworkServerBB(String name, String type) {
        super(name, type, SnappyNetworkServerBB.class);
    }

    /**
     * Creates a blackboard.
     */
    public static synchronized SnappyNetworkServerBB getBB() {
        if (blackboard == null) {
            blackboard =
                    new SnappyNetworkServerBB("SnappyNetworkServerBB", "rmi");
        }
        return blackboard;
    }
}