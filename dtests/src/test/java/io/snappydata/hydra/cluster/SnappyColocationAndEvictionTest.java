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

import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import hydra.Log;
import io.snappydata.Server;
import io.snappydata.ServerManager;
import parReg.ColocationAndEvictionTest;
import util.TestException;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by swati on 6/10/16.
 */
public class SnappyColocationAndEvictionTest {

    protected static final int HEAVY_OBJECT_SIZE_VAL = 500;

    /**
     * Task to verify PR bucket local destroy eviction.
     */
    public static synchronized void HydraTask_verifyEvictionLocalDestroy() throws SQLException {
        verifyEvictionLocalDestroy();
    }

    /**
     * Task to verify PR bucket local destroy eviction.
     */
    public static synchronized void verifyEvictionLocalDestroy() throws SQLException {
        boolean isSecurityEnabled = (Boolean) SnappyBB.getBB().getSharedMap().get("SECURITY_ENABLED");
        Properties locatorProps = new Properties();
        String locatorsList = SnappyTest.getLocatorsList("locators");
        locatorProps.setProperty("locators", locatorsList);
        locatorProps.setProperty("mcast-port", "0");
        if (isSecurityEnabled) {
            locatorProps.setProperty("user", "gemfire1");
            locatorProps.setProperty("password", "gemfire1");
        }
        Server server = ServerManager.getServerInstance();
        server.start(locatorProps);
        Set<LocalRegion> regions = Misc.getGemFireCache().getAllRegions();
        for (LocalRegion region : regions) {
            Log.getLogWriter().info("region name is : " + region.getName());
            if (region instanceof PartitionedRegion) {
                PartitionedRegion pr = (PartitionedRegion) region;
                pr.getRegion();
                verifyEvictionLocalDestroy(pr);
            }
        }
    }

    protected static void verifyEvictionLocalDestroy(PartitionedRegion aRegion) {
        if (aRegion.getLocalMaxMemory() == 0) {
            Log.getLogWriter().info(
                    "This is an accessor and hence eviction need not be verified");
            return;
        }

        double numEvicted = 0;

        EvictionAlgorithm evicAlgorithm = aRegion.getAttributes()
                .getEvictionAttributes().getAlgorithm();

        if (evicAlgorithm == EvictionAlgorithm.LRU_HEAP) {
            Log.getLogWriter().info("Eviction algorithm is HEAP LRU");
            numEvicted = ColocationAndEvictionTest.getNumHeapLRUEvictions();
            Log.getLogWriter().info("Evicted numbers :" + numEvicted);
            if (numEvicted == 0) {
                throw new TestException("No eviction happened in this test");
            }
        }

        Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
        Iterator iterator = bucketList.iterator();
        long count = 0;
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            BucketRegion localBucket = (BucketRegion) entry.getValue();
            if (localBucket != null) {
                Log.getLogWriter().info(
                        "The entries in the bucket " + localBucket.getName()
                                + " after eviction " + localBucket.entryCount());
                if (evicAlgorithm == EvictionAlgorithm.LRU_HEAP
                        && localBucket.entryCount() == 0) {
                    // May happen especially with lower heap (Bug 39715)
                    hydra.Log
                            .getLogWriter()
                            .warning(
                                    "Buckets are empty after evictions with local destroy eviction action");
                }
                if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY
                        || evicAlgorithm == EvictionAlgorithm.LRU_MEMORY)
                    count = count + localBucket.getCounter();
            }

        }
        if (evicAlgorithm == EvictionAlgorithm.LRU_ENTRY
                && count != aRegion.getAttributes().getEvictionAttributes()
                .getMaximum())
            throw new TestException("After Eviction total entries in region "
                    + aRegion.getName() + " expected= "
                    + aRegion.getAttributes().getEvictionAttributes().getMaximum()
                    + " but found= " + count);
        else if (evicAlgorithm == EvictionAlgorithm.LRU_MEMORY) {
            int configuredByteLimit = aRegion.getAttributes().getEvictionAttributes().getMaximum() * 1024 * 1024;
            final int ALLOWANCE = HEAVY_OBJECT_SIZE_VAL * HEAVY_OBJECT_SIZE_VAL; // allow 1 extra object
            int upperLimitAllowed = configuredByteLimit + ALLOWANCE;
            int lowerLimitAllowed = aRegion.getAttributes()
                    .getEvictionAttributes().getMaximum() * 1024 * 102;
            Log.getLogWriter().info("memlru, configuredByteLimit: " + configuredByteLimit);
            Log.getLogWriter().info("upperLimitAllowed (bytes): " + upperLimitAllowed);
            Log.getLogWriter().info("lowerLimitAllowed (bytes): " + lowerLimitAllowed);
            Log.getLogWriter().info("actual number of bytes: " + count);
            if ((count > upperLimitAllowed) || (count < lowerLimitAllowed)) {
                throw new TestException("After Eviction, configured memLRU bytes = " + configuredByteLimit +
                        " total number of bytes in region " + count);
            }
        }
    }
}
