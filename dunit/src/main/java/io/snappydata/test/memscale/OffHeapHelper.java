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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.offheap.MemoryAllocator;
import com.gemstone.gemfire.internal.offheap.MemoryBlock;
import com.gemstone.gemfire.internal.offheap.MemoryInspector;
import com.gemstone.gemfire.internal.offheap.OffHeapMemoryStats;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.DataAsAddress;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.RefCountChangeInfo;
import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.util.AEQHelper;
import io.snappydata.test.util.TestException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author lynng
 *
 */
public class OffHeapHelper {

  private final static int _totalNumberOffHeapObjects =0; // index 0
  private final static int _numberInlineValues = 1 ; // index 1
  private final static int _numRefCountProblems = 2; // index 2
  private final static int _lobCount =3; //index 3
  private final static int _totalNumberOnHeapObjects = 4 ;

  protected final static Logger logger =
      LogManager.getLogger(OffHeapHelper.class);

  /** Do consistency checks on off-heap memory. This is non-intrusive of the cache, meaning it is
   *  written to not rattle anything while the validation occurs, for example it won't cause eviction.
   * 
   *  This must be called when the system is quiet.
   *     * If there are persistent regions you cannot call this while asynchronous recovery of values 
   *       is running. To make the test run with synchronous recovery of values, set the property 
   *       gemfire.disk.recoverValuesSync=true.
   *     * Redundancy recovery for PartitionedRegions must not be running.
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
   *  
   *  @param checkRefCounts If true, then check that all refCounts are exactly 1. If false skip the
   *         refCount check. During validation, if any remote member is doing gets on data that lives
   *         in this member, the refCounts will be changing.
   */
  public static void verifyOffHeapMemoryConsistency(boolean checkRefCounts) {
    if (!isOffHeapMemoryConfigured()) {
      logger.info("No off-heap memory configured, skipping off-heap memory consistency checks");
      return;
    }
    Set<Region<?, ?>> allRegions = getAllRegions();
    if (allRegions == null) { // the cache is null (thus no regions) but proceed with checking what's in off-heap memory
      allRegions = new HashSet<Region<?, ?>>();
    }
    long beginObjectsStat = getOffHeapMemoryStats().getObjects();
    logger.info("Verifying off-heap memory consistency for " + allRegions.size() + " regions " +
        ((checkRefCounts) ? "including refCounts " : "NOT including refCounts"));

    final int MAX_ORPHANS_TO_REPORT = 20;
    final int MAX_REF_COUNT_PROBLEMS_TO_REPORT = 20;
    StringBuilder errStr = new StringBuilder();
    StringBuilder refCountErrStr = new StringBuilder();
    long[] statNumbers = new long[5];
    final long totalNumberOffHeapObjects ; // index 0
    final long numberInlineValues ; // index 1
    final long numRefCountProblems ; // index 2
    final long lobCount ; //index 3
    final long totalNumberOnHeapObjects ;//index4 
    List<OffHeapChunkInfo> chunkList = new ArrayList<OffHeapChunkInfo>();
    List<Chunk> lobChunkList = new ArrayList<Chunk>();
    for (Region<?, ?> aRegion : allRegions) { // iterate all regions
      String regionName = aRegion.getFullPath();
      logger.info("Verifying off-heap memory for " + regionName + ", enableOffHeapMemory for this region is " +
          aRegion.getAttributes().getEnableOffHeapMemory());
      LocalRegion localReg = (LocalRegion) aRegion;
      PartitionedRegion aPR = null;
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        aPR = (PartitionedRegion) aRegion;
      }
      Set<Object> offHeapKeys = new HashSet<Object>();
      Set<Object> onHeapKeys = new HashSet<Object>();
      Set<String> onHeapValueClasses = new HashSet<String>();
      if (aPR == null) {
        anaLyzeLocalRegion((LocalRegion) aRegion, offHeapKeys, onHeapKeys,
            onHeapValueClasses, statNumbers, chunkList, lobChunkList, checkRefCounts,
            refCountErrStr, errStr,MAX_ORPHANS_TO_REPORT,
            MAX_REF_COUNT_PROBLEMS_TO_REPORT);
      } else {
        PartitionedRegionDataStore prs = aPR.getDataStore();
        if (prs != null) {
          Set<BucketRegion> brs = prs.getAllLocalBucketRegions();
          if (brs != null) {
            for (BucketRegion br : brs) {
              if (br != null) {
                logger.info("Verifying bucket " + br.getFullPath());
                anaLyzeLocalRegion(br, offHeapKeys, onHeapKeys,
                    onHeapValueClasses, statNumbers, chunkList, lobChunkList, checkRefCounts,
                    refCountErrStr, errStr, MAX_ORPHANS_TO_REPORT,
                    MAX_REF_COUNT_PROBLEMS_TO_REPORT);
              }
            }
          }
        }
      }
    } // done iterating all regions

    numberInlineValues = statNumbers[_numberInlineValues];
    numRefCountProblems = statNumbers[_numRefCountProblems];
    lobCount = statNumbers[_lobCount];
    totalNumberOnHeapObjects = statNumbers[_totalNumberOnHeapObjects];
    totalNumberOffHeapObjects = statNumbers[_totalNumberOffHeapObjects] + lobCount;

    if (refCountErrStr.length() > 0) {
      errStr.append(refCountErrStr);
      if (numRefCountProblems > MAX_REF_COUNT_PROBLEMS_TO_REPORT) {
        errStr.append("...<"
            + (numRefCountProblems - MAX_REF_COUNT_PROBLEMS_TO_REPORT)
            + " more refCount problems>...\n");
      }
    }

    // Verify whether the stat for the number of objects stored off-heap is the
    // same as the number of off-heap
    // objects that were reachable through all regions. This check looks for
    // leaks as we might have objects in
    // off-heap memory that we were unable to reach by iterating (orphans).
    int objects = getOffHeapMemoryStats().getObjects();
    long numUnreachable = 0;
    if (objects != totalNumberOffHeapObjects) {
      errStr.append("Total number of off-heap objects reachable via regions is " + totalNumberOffHeapObjects +
          ((lobCount == 0) ? "" : " (including " + lobCount + " SqlFire Lobs)") +
          ", but the number of objects stored off-heap according to stats is " + objects + 
          " (difference of " + Math.abs(objects - totalNumberOffHeapObjects) + ")\n");
      if (objects > totalNumberOffHeapObjects) {
        numUnreachable = objects - totalNumberOffHeapObjects;
      }
    }
    boolean foundOrphans = (numUnreachable > 0);

    // Use the internal free and live lists to look for orphans
    SimpleMemoryAllocatorImpl offHeapStore = SimpleMemoryAllocatorImpl.getAllocator();
    if (offHeapStore != null) {
      List<Chunk> orphanedChunks = offHeapStore.getLostChunks();
      orphanedChunks.removeAll(lobChunkList);
      int numOrphanedChunks = orphanedChunks.size();
      if (numOrphanedChunks != numUnreachable) {
        errStr
        .append("Number of off-heap values unreachable through regions is "
            + numUnreachable
            + " but number of orphaned chunks detected with internal free and live lists is "
            + numOrphanedChunks + "\n");
      }
      if (numOrphanedChunks > 0) {
        foundOrphans = true;
        String aStr = numOrphanedChunks + " orphaned chunks detected with internal free and live lists";
        logger.info(aStr);
        errStr.append(aStr + "\n");
        for (int i = 0; i < orphanedChunks.size(); i++) {
          Chunk aChunk = orphanedChunks.get(i);
          aStr = "orphaned @" + Long.toHexString(aChunk.getMemoryAddress()) + " rc=" + aChunk.getRefCount();
          List<RefCountChangeInfo> info = SimpleMemoryAllocatorImpl.getRefCountInfo(aChunk.getMemoryAddress());
          String logStr;
          if (info != null) {
            logStr = aStr + " history=" + info;
          } else {
            logStr = aStr;
          }
          logger.info(logStr);
          if (i < MAX_ORPHANS_TO_REPORT) {
            errStr.append(aStr + "\n");
          }
        }
        if (orphanedChunks.size() > MAX_ORPHANS_TO_REPORT) {
          errStr.append("...<" + (orphanedChunks.size() - MAX_ORPHANS_TO_REPORT) + " more orphans>...\n");
        }
      }
    }
    if (foundOrphans) {
      dumpOffHeapOrphans();
    }

    // verify all chunks reachable through regions
    errStr.append(verifyChunks(chunkList));

    if (errStr.length() > 0) {
      logger.info(errStr.toString());
    }

    long totalVerified = totalNumberOffHeapObjects + totalNumberOnHeapObjects + numberInlineValues;
    logger.info("Verified a total of " + totalVerified + " objects in " + allRegions.size() + " regions including "
        + totalNumberOffHeapObjects + " off-heap objects, " + totalNumberOnHeapObjects + " on-heap objects, and "
        + numberInlineValues + " in-line values");
    if (errStr.length() > 0) {
      logger.info(errStr.toString());
      long finalObjectsStat = getOffHeapMemoryStats().getObjects();
      if (beginObjectsStat != finalObjectsStat) {
        errStr.insert(0, "Off-heap memory was not stable during off-heap memory validation. " +
            "Number of off-heap objects at the beginning of validation: " + beginObjectsStat +
            ", number of off-heap objects at the end of validation: " + finalObjectsStat + "\n");

      }
      throw new TestException(errStr.toString());
    }
  }

  private static void anaLyzeLocalRegion(LocalRegion localReg, 
      Set<Object> offHeapKeys,
      Set<Object> onHeapKeys, 
      Set<String> onHeapValueClasses,
      long[] statNumbers, 
      List<OffHeapChunkInfo> chunkList,
      List<Chunk> lobChunkList,
      boolean checkRefCounts, 
      StringBuilder refCountErrStr,
      StringBuilder errStr,
      final int MAX_ORPHANS_TO_REPORT,
      final int MAX_REF_COUNT_PROBLEMS_TO_REPORT) {
    String regionName = localReg.getFullPath();
    for (Object key: localReg.keySet()) { // iterate all keys/values in the region
      // get the value for the key
      Object value = null;

      RegionEntry entry = localReg.getRegionEntry(key);
      if (entry == null) {
        throw new TestException("For key " + key + " in region " + regionName + ", LocalRegion.getRegionEntry(key) returned " + entry);
      }
      value = entry._getValue();
      if (value instanceof Chunk) { // this value is stored off-heap
        offHeapKeys.add(key);
        statNumbers[_totalNumberOffHeapObjects]++;
        Chunk aChunk = (Chunk)value;
        OffHeapChunkInfo info = new OffHeapChunkInfo(regionName, key, aChunk.getMemoryAddress(), aChunk.getSize());
        chunkList.add(info);
        OffHeapHelperVersionHelper.checkIsAllocated(aChunk);

        // verify refCount
        if (checkRefCounts) {
          int refCount = aChunk.getRefCount();
          if (refCount != 1) {
            statNumbers[_numRefCountProblems]++;
            if (statNumbers[_numRefCountProblems] <= MAX_REF_COUNT_PROBLEMS_TO_REPORT) {
              refCountErrStr.append(localReg.getFullPath() + " key " + key + " has off-heap refCount " + refCount + 
                  " @" + Long.toHexString(aChunk.getMemoryAddress()) + "\n");
              if (SimpleMemoryAllocatorImpl.trackReferenceCounts()) {
                List<RefCountChangeInfo> history = SimpleMemoryAllocatorImpl.getRefCountInfo(aChunk.getMemoryAddress());
                if (history != null) {
                  String logStr = "extraRefs for @" + Long.toHexString(aChunk.getMemoryAddress()) + " rc=" + refCount + " history=" + history;
                  logger.info(logStr);
                } else {
                  logger.info("No history for @" + Long.toHexString(aChunk.getMemoryAddress()));
                }
              }
            }
          }
        }

        // in a GemFireXD test, check for lobs in off-heap memory
        List<Chunk> aList = getSqlLobChunks(aChunk, regionName, key);
        for (Chunk lobChunk: aList) {
          info = new OffHeapChunkInfo(regionName, key, lobChunk.getMemoryAddress(), lobChunk.getSize());
          chunkList.add(info);

          // we can safely check the ref counts on the lobs always and not consider the value of checkRefCounts
          // because for a lob the refcount should always be one
          int refCount = lobChunk.getRefCount();
          if (refCount != 1) {
            statNumbers[_numRefCountProblems]++;
            if (statNumbers[_numRefCountProblems] <= MAX_REF_COUNT_PROBLEMS_TO_REPORT) {
              refCountErrStr.append(localReg.getFullPath() + " key " + key + " lob at address " + lobChunk.getMemoryAddress() +
                  " has off-heap refCount " + refCount + "\n");
              List<RefCountChangeInfo> history = SimpleMemoryAllocatorImpl.getRefCountInfo(lobChunk.getMemoryAddress());
              if (history != null) {
                String logStr = "extraRefs for @" + Long.toHexString(lobChunk.getMemoryAddress()) + " rc=" + refCount + " history=" + history;
                logger.info(logStr);
              }
            }
          }
        }
        lobChunkList.addAll(aList);
        statNumbers[_lobCount] += aList.size();
      } else if (value instanceof DataAsAddress) { // value is neither on heap nor off heap
        statNumbers[_numberInlineValues]++;
      } else if ((value != Token.INVALID) && (value != Token.LOCAL_INVALID) && (value != null)) { // value not invalid, not null; this value is not stored off-heap
        onHeapKeys.add(key);
        onHeapValueClasses.add(value.getClass().getName());
      }
    }
    statNumbers[_totalNumberOnHeapObjects] += onHeapKeys.size();

    // verify whether values are off-heap or not
    if (localReg.getAttributes().getEnableOffHeapMemory()) { // off heap enabled; all values should be off-heap
      if (onHeapKeys.size() > 0 ) {
        //check if it is a HDFS region, if so ingore the check
        Class<? extends LocalRegion> clazz = localReg.getClass();
        boolean isHDFS = false;
        try {
          Method isHDFSRegionMethod = clazz.getDeclaredMethod("isHDFSRegion", (Class<?> [])null);
          isHDFSRegionMethod.setAccessible(true);
          isHDFS =  ((Boolean)isHDFSRegionMethod.invoke(localReg, (Object[])null)).booleanValue();
        }catch(Throwable th) {
          throw new RuntimeException("Could not determine if it is a HDFS region",th);
        }
        if(!isHDFS) {
          errStr.append(localReg.getFullPath() + " has off-heap enabled, but the following " + onHeapKeys.size() + 
            " keys had values not found in off-heap memory: " + onHeapKeys + 
            ", set of value classes for those keys: " + onHeapValueClasses + "\n");
        }
      }
    } else { // off heap NOT enabled; no values should be found off-heap
      if (offHeapKeys.size() > 0) {
        errStr.append(localReg.getFullPath() + " has off-heap disabled, but the following keys had values " +
            " found in off-heap memory: " + offHeapKeys + "\n");
      }
    }
  }

  /** For a GemFireXD test, get the lob chunks for rows stored in off-heap memory. Lobs are stored as references
   *  from the base row in GemFireXD.
   *  We must use reflection for this because GF core test code does not have access to GemFireXD classes (test or product).
   *  
   *  @param value The off-heap chunk of an object (the base row in GemFireXD).
   *  @param regionName The name of the region (table) referencing aChunk.
   *  @param key The key associated with aChunk.
   *  @return A List of Chunks where each Chunk is the off-heap momory chunk of aChunk's lobs, or an empty List if none.
   */
  private static List<Chunk> getSqlLobChunks(Chunk value, String regionName, Object key) {
    try {
      Class sqlHelperClass = Class.forName("sql.sqlutil.SqlOffHeapHelper");
      Method method = sqlHelperClass.getDeclaredMethod("getLobChunks", new Class[] {Chunk.class, String.class, Object.class});
      Object returnObj = method.invoke(null,  value, regionName, key);
      return (List<Chunk>)returnObj;
    } catch (ClassNotFoundException e) { // class not available for core GemFire tests
      return new ArrayList();
    } catch (IllegalArgumentException e) {
      throw new TestException(DistributedTestBase.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(DistributedTestBase.getStackTrace(e));
    } catch (InvocationTargetException e) {
      Throwable lastInChain = e;
      while (lastInChain.getCause() != null) {
        lastInChain = lastInChain.getCause();
      }
      if ((lastInChain instanceof ClassNotFoundException) || (lastInChain instanceof NoClassDefFoundError)) { 
        // if gemfirexd was built but this is a gf core test
        // that has the test classes (including gemfirexd test classes) on the classpath, but does not have the
        // gemfirexd product jar on the classpath, then invoking the sql test method will try to load gemfirexd
        // product classes, thus we can get a ClassNotFoundException here; since this is OK for gf core tests, allow it
        return new ArrayList();
      }
      throw new TestException(DistributedTestBase.getStackTrace(e));
    } catch (SecurityException e) {
      throw new TestException(DistributedTestBase.getStackTrace(e));
    } catch (NoSuchMethodException e) {
      throw new TestException(DistributedTestBase.getStackTrace(e));
    }
  }

  /** use internal product calls to determine who the orphans are and log them
   * 
   */
  public static void dumpOffHeapOrphans() {
    MemoryAllocator store = SimpleMemoryAllocatorImpl.getAllocator();
    if (store == null) {
      logger.info("Not dumping off-heap orphans, offHeapStore is " + store);
      return;
    }
    MemoryInspector inspector = store.getMemoryInspector();
    List<MemoryBlock> orphans = inspector.getOrphans();
    for (MemoryBlock block : orphans) {
      logger.error("Orphaned MemoryBlock: " + block.toString());
    }
  }

  /** Closes all offheap regions in the cache and waits for off-heap memory to be empty (closing
   *  a region asynchronously releases the off-heap memory it held, so we want to wait
   *  for that to finish).
   * 
   *  This is used to look for off-heap memory leaks. After all the regions are closed
   *  calling verifyOffHeapMemoryConsistency will detect any objects still in off-heap
   *  which indicates a leak.
   */
  public static synchronized void closeAllRegions() {
    closeAllOffHeapRegions(); // leave closeAllRegions in place since many bugs are filed with this as a close task
  }

  /** Closes all off-heap regions in the cache and waits for off-heap memory to be empty (closing
   *  a region asynchronously releases the off-heap memory it held, so we want to wait
   *  for that to finish).
   * 
   *  This is used to look for off-heap memory leaks. After all the regions are closed
   *  calling verifyOffHeapMemoryConsistency will detect any objects still in off-heap
   *  which indicates a leak.
   */
  public static synchronized void closeAllOffHeapRegions() {
    Cache theCache = GemFireCacheImpl.getInstance();
    if (theCache == null) {
      logger.info("The cache is null");
      return;
    }
    Set<Region<?, ?>> regionSet = getAllRegions();
    if (regionSet.size() > 0) {

      for (Region aRegion: regionSet) {     
        if (aRegion.getAttributes().getEnableOffHeapMemory()) {
          logger.info("Closing " + aRegion.getFullPath());
          try {
            aRegion.close();
            logger.info("Closed " + aRegion.getFullPath());
          } catch (RegionDestroyedException e) {
            logger.info(aRegion.getFullPath() + " was already destroyed");
          }
        } else {
          logger.info("Not closing " + aRegion.getFullPath() + " because off-heap memory is not enabled for this region");

        }
      }

      //now wait for off-heap memory to empty; objects are removed asynchronously after closing a region
      if (isOffHeapMemoryConfigured()) {
        long numObjectsInOffHeapMemory = getOffHeapMemoryStats().getObjects();
        int retryCount = 0;
        while (numObjectsInOffHeapMemory != 0 && retryCount < 31) {
          logger.info("Waiting for off-heap memory to empty, current number of objects is " + numObjectsInOffHeapMemory);
          DistributedTestBase.sleepForMs(2000);
          retryCount++;
          numObjectsInOffHeapMemory = getOffHeapMemoryStats().getObjects();
        }
        if (numObjectsInOffHeapMemory > 0) {
          logger.error("Number of objects in off-heap memory: " + numObjectsInOffHeapMemory);
        } else {
          logger.info("Number of objects in off-heap memory: " + numObjectsInOffHeapMemory);
        }
      }
    }
  }

  /** Verify the given List of OffHeapChunkInfo instances. 
   * 
   * @param chunkList A List of OffHeapChunkInfo instances.
   */
  private static String verifyChunks(List<OffHeapChunkInfo> chunkList) {
    Collections.sort(chunkList);
    logger.info("Verifying " + chunkList.size() + " off-heap memory chunks");
    StringBuilder errStr = new StringBuilder();
    List<Integer> freeMemoryIndexes = new ArrayList<Integer>(); //index into chunk list of chunks that have a free memory segment following it
    long totalBytesConsumedInChunks = 0;
    for (int i = 0; i < chunkList.size(); i++) {
      OffHeapChunkInfo currentInfo = chunkList.get(i);
      long firstAddress = currentInfo.getFirstMemoryAddress();
      if ((firstAddress & 7) != 0) {
        errStr.append("Off-heap memory address was not 8 byte aligned: " + currentInfo + "\n");
      }
      if (firstAddress < 1024) {
        throw new IllegalStateException("Off-heap memory address was smaller than expected " + currentInfo + "\n");
      }
      totalBytesConsumedInChunks += currentInfo.getNumberBytes();
      if (i > 0) { // not the first chunk
        OffHeapChunkInfo previousInfo = chunkList.get(i-1);
        if (firstAddress == previousInfo.getFirstMemoryAddress()) {
          errStr.append("<" + currentInfo + "> is referencing the same off-heap memory address as <" + previousInfo + ">\n");
        } else if (firstAddress <= previousInfo.getLastMemoryAddress()) {
          errStr.append("<" + currentInfo + "> overlaps off-heap memory with <" + previousInfo + ">\n");
        } else if (previousInfo.getLastMemoryAddress() + 1 != firstAddress) {  
          freeMemoryIndexes.add(i-1);
        }
      }
    }
    StringBuilder aStr = new StringBuilder();
    long totalFreeMemoryInBytes = 0;
    long minFreeMemorySizeInBytes = Long.MAX_VALUE;
    long maxFreeMemorySizeInBytes = 0;
    final int maxChunksToLog = Math.min(freeMemoryIndexes.size(), 20);
    for (int i = 0; i < freeMemoryIndexes.size(); i++) {
      int chunkListIndex = freeMemoryIndexes.get(i);
      OffHeapChunkInfo chunkBeforeFreeMemory = chunkList.get(chunkListIndex);
      OffHeapChunkInfo chunkAfterFreeMemory = chunkList.get(chunkListIndex+1);
      long memoryAddressOfFreeMemory = chunkBeforeFreeMemory.getLastMemoryAddress() + 1;
      long freeMemorySizeInBytes = chunkAfterFreeMemory.getFirstMemoryAddress() - memoryAddressOfFreeMemory;
      totalFreeMemoryInBytes += freeMemorySizeInBytes;
      minFreeMemorySizeInBytes = Math.min(minFreeMemorySizeInBytes, freeMemorySizeInBytes);
      maxFreeMemorySizeInBytes = Math.max(maxFreeMemorySizeInBytes, freeMemorySizeInBytes);
      if (i+1 <= maxChunksToLog) {
        aStr.append("  " + (i+1) + ": free memory of size " + freeMemorySizeInBytes + " bytes between chunk " + (chunkListIndex) + 
            " <" + chunkBeforeFreeMemory + "> and next chunk <" + chunkAfterFreeMemory + ">\n");
      }
    }
    logger.info(chunkList.size() + " chunks consumed " + totalBytesConsumedInChunks + " bytes of off-heap memory");

    if (minFreeMemorySizeInBytes < 8) {
      errStr.append("The minimum free memory size is " + minFreeMemorySizeInBytes + ", but expected it to be >= 8\n");
    }

    if (freeMemoryIndexes.size() == 0) {
      logger.info("Found 0 free memory segments between chunks");
    } else {
      double average = totalFreeMemoryInBytes / freeMemoryIndexes.size();
      logger.info("Found " + freeMemoryIndexes.size() + " free memory segments between chunks; free memory totals " +
          totalFreeMemoryInBytes + " bytes, min free memory size " + minFreeMemorySizeInBytes + " bytes, max free memory size " +
          maxFreeMemorySizeInBytes + " bytes, average free memory size " + average + " bytes\nFirst " + maxChunksToLog +
          " free memory chunks:\n" + aStr);
    }
    return errStr.toString();
  }

  /** Return a Set of all regions defined in this member.
   * 
   * @return A Set of all regions defined in this member.
   */
  public static Set<Region<?, ?>> getAllRegions() {
    // get all regions
    Cache theCache = GemFireCacheImpl.getInstance();
    if (theCache == null) {
      logger.info("There are no regions in this member, cache is null");
      return null;
    }
    Set<Region<?, ?>> rootRegions = theCache.rootRegions();
    Set<Region<?, ?>> allRegions = new HashSet<Region<?, ?>>();
    allRegions.addAll(rootRegions);
    for (Region<?, ?> aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }
    return allRegions;
  }

  /** Verify that the given list of full region path names have off-heap memory enabled
   * 
   * @param regionNames A List of full region path names, or null of all regions are
   *                    expected to have off-heap enabled.
   */
  public static void verifyRegionsEnabledWithOffHeap(List<String> regionNames) {
    StringBuilder errStr = new StringBuilder();
    boolean expectOffHeapEnabled = false;
    Set<Region<?, ?>> allRegions = getAllRegions();
    for (Region aRegion: allRegions) {
      boolean offHeapEnabled = aRegion.getAttributes().getEnableOffHeapMemory();
      if (regionNames == null) {
        expectOffHeapEnabled = true;
      } else {
        expectOffHeapEnabled = regionNames.contains(aRegion.getFullPath());
      }
      if (expectOffHeapEnabled != offHeapEnabled) {
        errStr.append("Expected attributes for " + aRegion.getFullPath() + " to have enableOffHeapMemory " +
            expectOffHeapEnabled + ", but it is " + offHeapEnabled + "\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
  }
  
  /** Determines if this member has off-heap memory currently allocated. This returns true
   *  if off-heap memory is present even if the cache is closed.
   * 
   * @return True if this member has off-heap memory currently allocated, false otherwise.
   */
  public static boolean isOffHeapMemoryConfigured() {
    try {
      MemoryAllocator offHeapStore = SimpleMemoryAllocatorImpl.getAllocator();
      return (offHeapStore != null);
    } catch (CacheClosedException e) { // CacheClosed is thrown if the cache is closed AND no off-heap memory is present
                                       // but is not thrown if the cache is closed and off-heap memory is present
      String errStr = e.toString();
      if (errStr.contains("Off Heap memory allocator does not exist")) {
        return false;
      }
      else throw e;
    }
  }

  /** Return the off-heap memory stats object for this member
   * 
   * @return The off-heap memory stats for this member.
   */
  public static OffHeapMemoryStats getOffHeapMemoryStats() {
    MemoryAllocator offHeapStore = SimpleMemoryAllocatorImpl.getAllocator();
    if (offHeapStore == null) {
      throw new TestException("Cannot get off-heap memory stats because the offHeapStore is null");
    }
    OffHeapMemoryStats offHeapStats = offHeapStore.getStats();
    if (offHeapStats == null) {
      throw new TestException("The off-heap stats is null");
    }
    return offHeapStats;
  }

  /** Wait for all async event queues (including wan queues) in this member to drain. Does not return until
   *  all wan queues are empty.
   */
  public static void waitForWanQueuesToDrain() {
    AEQHelper.waitForAsyncEventQueuesToDrain();
  }
}
