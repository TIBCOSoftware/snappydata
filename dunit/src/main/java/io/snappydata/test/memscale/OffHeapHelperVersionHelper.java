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
package io.snappydata.test.memscale;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;

/**
 * @author lynng
 *
 */
public class OffHeapHelperVersionHelper {

  public static void checkIsAllocated(Chunk aChunk) {
    aChunk.checkIsAllocated();
  }

  /** Do consistency checks on off-heap memory. This is non-intrusive of the cache, meaning it is
   *  written to not rattle anything while the validation occurs, for example it won't cause eviction.
   * 
   *  This must be called when the system is quiet.
   *     * If there are persistent regions you cannot call this while asynchronous recovery of values 
   *       is running. To make the test run with synchronous recovery of values, set the property 
   *       gemfire.disk.recoverValuesSync=true.
   *     * Redundancy recovery for PartitionedRegions must not be running.
   *     * wan queues must be drained
   *  
   *  This method iterates all keys and values of all regions. If no regions are defined, this still provides 
   *  consistency checks on off-heap memory as it verifies that there are no objects in off-heap as compared 
   *  to off-heap stats; this check could indicate a leak if objects are found off-heap when no regions are defined.
   *  
   *  Note that this validation trusts the enableOffHeapMemory region attribute in that it will verify
   *  that off-heap enabled regions have all entries off-heap and that region with off-heap disabled will
   *  have all entries in heap. To be completely thorough, the test can verify that each region's off-heap
   *  enabled attribute is correct before calling this method. See OffHeapHelper.verifyRegionsEnabledWithOffHeap(...)
   *  to verify this attribute on regions.
   *  
   *  If no off-heap memory was configured for this member (ie no off-heap memory was specified with
   *  GemFirePrms.offHeapMemorySize), then this method takes an early return. 
   */
  public static void verifyOffHeapMemoryConsistency() {
    OffHeapHelper.verifyOffHeapMemoryConsistency(false);
  }
}

