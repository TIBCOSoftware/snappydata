/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package io.snappydata.test.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderEventProcessor;
import io.snappydata.test.dunit.DistributedTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lynng
 * Helper class for AsyncEventQueues.
 * 
 */
public class AEQHelper {

  protected final static Logger logger = LoggerFactory.getLogger(AEQHelper.class);

  /** Wait for async event queues in this member to drain. This wait is done
   *  once for all threads that call this concurrently.
   * 
   */
  static long waitEndTime = 0;
  public static void waitForAsyncEventQueuesToDrainOnce() {
    long waitRequestedTime = System.currentTimeMillis();
    logger.info("Wait for async event queues requested time is " + waitRequestedTime + ", end time is : " + waitEndTime);
    synchronized (AEQHelper.class) {
      if (waitRequestedTime > waitEndTime) { // do the wait
        try {
          waitForAsyncEventQueuesToDrain();
        } finally {
          waitEndTime = System.currentTimeMillis();
          logger.info("Wait end time is " + waitEndTime);
        }
      } else {
        logger.info("This thread is not waiting for async event queues to drain because it was done by another thread in this member");
      }
    }
  }

  /** Wait for all async event queues in this member to drain. This includes wan
   *  queues which are async event queues. Does not return until all queues are empty.
   */
  public static void waitForAsyncEventQueuesToDrain() {
    Cache theCache = GemFireCacheImpl.getInstance();
    if (theCache == null) {
      logger.info("No async event queues to wait for; cache is null");
      return;
    }
    Map<String,Region> queueRegions = new HashMap<String,Region>();
    final GemFireCacheImpl gfci = (GemFireCacheImpl) theCache;

    // get the queues from gateway senders
    Set<GatewaySender> senderSet = gfci.getAllGatewaySenders();
    if ((senderSet == null) || (senderSet.size() == 0)) {
      logger.info("No gateway senders in this member");
    } else {
      // gather up the queue regions from the gateway senders
      for (GatewaySender sender : senderSet) {
        List<Region> regions = getGatewaySenderRegions(sender);
        for (Region region : regions) {
          queueRegions.put(region.getFullPath(), region);
        }
      }
    }

    // gather up async event queues
    Set<AsyncEventQueue> queues = gfci.getAsyncEventQueues();
    for (AsyncEventQueue queue : queues) {
      List<Region> regions = getGatewaySenderRegions(((AsyncEventQueueImpl)queue).getSender());
      for (Region region : regions) {
        queueRegions.put(region.getFullPath(), region);
      }
    }

    if (queueRegions.size() == 0) {
      logger.info("No async event queue regions found in this member.");
    }

    // wait for the queue regions to empty
    final int MS_TO_SLEEP = 1000;
    final int MAX_WAITS = 60*5; // wait no more than 5 minutes to prevent junit hangs
    int waits = 0;
    while (waits++ < MAX_WAITS) { 
      boolean allQueuesEmpty = true;
      for (Map.Entry<String,Region> entry : queueRegions.entrySet()) {
        String regionName = entry.getKey();
        if (!entry.getValue().isDestroyed()) {
          int currentRegionSize = entry.getValue().size();
          logger.info("Queue size for " + regionName + ": " + currentRegionSize);
          if (currentRegionSize != 0) {
            allQueuesEmpty = false;
          }
        }
      }
      if (allQueuesEmpty) {
        logger.info("All async event queues are empty");
        break;
      }
      DistributedTestBase.sleepForMs(MS_TO_SLEEP);
    }
    if (waits == MAX_WAITS) {
      throw new TestException("queues never drained");
    }

    /*
    // now all queues are empty; do a consistency check on the queues to look for bug 49196
    // since queues are empty, this check should be quick and not consume a lot of memory
    // (unless we detect 49196!)
    for (Region aRegion: queueRegions.values()) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        try {
          ParRegUtil.verifyBucketCopies(aRegion, aRegion.getAttributes()
              .getPartitionAttributes().getRedundantCopies());
        } catch (TestException e) {
          String errStr = e.getMessage();
          throw new TestException("Bug 49196 detected for " + aRegion.getFullPath() + ": " + errStr);
        }
      }
    }
    */
  }

  /** Given a gateway sender, return a List of region queues for it.
   * 
   * @param sender Find the region queues for this gateway sender.
   * @return The list of region queues for this sender.
   */
  private static List<Region> getGatewaySenderRegions(GatewaySender sender) {
    List<Region> regions = new ArrayList<Region>();
    AbstractGatewaySenderEventProcessor processor = ((AbstractGatewaySender) sender).getEventProcessor();
    if (processor == null) {
      return regions;
    }
    if (processor instanceof SerialGatewaySenderEventProcessor) {
      // parallel="false" dispatcher-threads="1"
      regions.add(processor.getQueue().getRegion());
      logger.info("Serial gateway sender " + sender.getId() + " uses region named " + processor.getQueue().getRegion().getFullPath());
    } else if (processor instanceof ParallelGatewaySenderEventProcessor) {
      // parallel="true" dispatcher-threads="1"
      ParallelGatewaySenderQueue queue = (ParallelGatewaySenderQueue) processor.getQueue();
      Set<PartitionedRegion> regSet = queue.getRegions();
      regions.addAll(regSet);
      StringBuilder sb = new StringBuilder();
      sb.append("Parallel gateway sender " + sender.getId() + " uses " + regSet.size() + " regions");
      for (Region aRegion: regSet) {
        sb.append(" " + aRegion.getFullPath());
      }
      logger.info(sb.toString());
    } else if (processor instanceof ConcurrentSerialGatewaySenderEventProcessor) {
      // parallel="false" dispatcher-threads="2"
      ConcurrentSerialGatewaySenderEventProcessor cProcessor = (ConcurrentSerialGatewaySenderEventProcessor) processor;
      Set<RegionQueue> queues = cProcessor.getQueues();
      StringBuilder sb = new StringBuilder();
      sb.append("Concurrent serial gateway sender " + sender.getId() + " uses " + queues.size() + " regions");
      for (RegionQueue queue : queues) {
        Region aRegion = queue.getRegion();
        regions.add(aRegion);
        sb.append(" " + aRegion.getFullPath());
      }
      logger.info(sb.toString());
    } else if (processor instanceof ConcurrentParallelGatewaySenderEventProcessor) {
      // parallel="true" dispatcher-threads="2"
      regions.addAll(((ConcurrentParallelGatewaySenderQueue)processor.getQueue()).getRegions());
    //  logger.info("Concurrent parallel gateway sender " + sender.getId() + " uses region named " + processor.getQueue().getRegion().getFullPath());
    } else {
      throw new TestException("Unknown processor class: " + processor.getClass().getName());
    }
    return regions;
  }
}
