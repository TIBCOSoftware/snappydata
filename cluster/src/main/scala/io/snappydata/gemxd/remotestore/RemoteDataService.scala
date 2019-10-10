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

package io.snappydata.gemxd.remotestore

import java.io._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.Comparator
import java.util.concurrent.{LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.{Predicate, ToLongFunction}

import com.gemstone.gemfire.internal.LogWriterImpl
import com.gemstone.gemfire.internal.cache.{DiskEntry, DiskStoreImpl, GemFireCacheImpl, GemfireCacheHelper}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class ExtendedStorageAttributes(ds: DiskStoreImpl,
  extType: String, cacheDir: String, maxCacheSizeInMB: Long)

final class RemoteDataService(
  extStorageAttrs: ExtendedStorageAttributes, cache: GemFireCacheImpl) extends Logging {

  val remoteStorageFetchThreadGroup: ThreadGroup =
    LogWriterImpl.createThreadGroup("Remote Storage Value fetcher", cache.getLoggerI18n)
  val threadFactory: ThreadFactory =
    GemfireCacheHelper.createThreadFactory(remoteStorageFetchThreadGroup, "Remote Data Service")
  val valueFetcherPool = new ThreadPoolExecutor(1, maximumParallelFetch,
    60, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](100),
    threadFactory, new ThreadPoolExecutor.CallerRunsPolicy)

  // TODO: Put some property based
  lazy val maximumParallelFetch = 10

  val cleanerLock: ReentrantReadWriteLock = new ReentrantReadWriteLock()

  val ds: DiskStoreImpl = extStorageAttrs.ds

  // Start the cleaner thread
  val cleaner = new CacheCleaner(this)
  valueFetcherPool.submit(cleaner)

  // TODO: Better toString
  override def toString: String = s"${extStorageAttrs.extType}"

  lazy val entryCache: Cache[String, ValueObject] = {
    // Keeping the cache size very big. Actual data cached will depend on the amount
    // of disk space configured for keeping the cached data.
    CacheBuilder.newBuilder().maximumSize(10000000).build[String, ValueObject]()
  }

  def getExternalStorageAttributes: ExtendedStorageAttributes = extStorageAttrs

  lazy val client: RemoteDataClient = null

  var currentCacheSize: AtomicLong = calculateInitialSizeUsed()

  def calculateInitialSizeUsed(): AtomicLong = {
    var sz: Long = 0
    var count = 0
    val cacheDir = new File(extStorageAttrs.cacheDir)
    for (file <- cacheDir.listFiles) {
      // TODO: change when exact file pattern name is decided
      if (file.isFile && file.getName.contains("oplog")) {
        sz = file.length
        count += 1
      }
    }
    logDebug(s"cache directory has $count files of total size $sz for $this")
    new AtomicLong(sz)
  }

  def scheduleWatch(watchList: ListBuffer[ValueObject],
                    str: String, resultStream: BatchResultStream[ValueObject]): Unit = {
    val watcher = ValueWatcher(watchList, str, resultStream)
    valueFetcherPool.submit(watcher)
  }

  def getValues(diskptrs: Array[DiskEntry]): BatchResultStream[ValueObject] = {
    cleanerLock.readLock().lock()
    try {
      val numrequests = diskptrs.length
      val resultStream = new BatchResultStream[ValueObject](numrequests)
      val watchList = new mutable.ListBuffer[ValueObject]()
      diskptrs.foreach(dp => {
        val filename = RemoteDataService.getFileName(ds, dp)
        var vobj = entryCache.getIfPresent(filename)
        if (vobj == null) {
          this.entryCache.synchronized {
            vobj = entryCache.getIfPresent(filename)
            if (vobj == null) {
              vobj = ValueObject(filename, dp, this)
              entryCache.put(filename, vobj)
              // submit a task in executor service to remote pull and write in file
              val newValueFetcher = ValueFetcher(vobj, filename, "", this)
              valueFetcherPool.submit(newValueFetcher)
            }
          }
        }
        val fp = Paths.get(filename)
        if (Files.exists(fp)) {
          resultStream.addResult(vobj)
        } else {
          watchList += vobj
        }
      })
      if (watchList.nonEmpty) {
        scheduleWatch(watchList, "", resultStream)
      }
      resultStream
    } finally {
      cleanerLock.readLock().unlock()
    }
  }

  case class ValueFetcher(vobj: ValueObject, filename: String,
                     cacheDir: String, rds: RemoteDataService) extends Runnable {
    override def run(): Unit = {
      val len = vobj.ptr.getDiskId.getValueLength
      val offSet = vobj.ptr.getDiskId.getOffsetInOplog
      val range = Array[Long](offSet, offSet + len)
      // TODO: Exception handling
      val bytes = rds.client.getValues(filename, range)
      // Write the bytes in the file and cache and update the cache
      val bos = new BufferedOutputStream(new FileOutputStream(filename))
      bos.write(bytes)
      bos.close()
      rds.currentCacheSize.addAndGet(bytes.length)
    }
  }

  case class ValueWatcher(watchList: mutable.ListBuffer[ValueObject], cacheDir: String,
                     batchResult: BatchResultStream[ValueObject]) extends Runnable {
    override def run(): Unit = {
      var list = watchList
      val addedList: mutable.ListBuffer[ValueObject] = new mutable.ListBuffer[ValueObject]()
      while (list.nonEmpty) {
        logDebug(s"remaining values are ${list.size}")
        list.foreach(v => {
          val fp = Paths.get(v.filename)
          if (Files.exists(fp)) {
            logDebug(s"data arrived for ${v.filename}")
            batchResult.addResult(v)
            addedList += v
          }
        })
        list = list.filterNot(addedList.contains(_))
        Thread.sleep(50) // TODO: 50 ms seems good
      }
      logDebug(s"all pending values arrived")
    }
  }

  class CacheCleaner(val rds: RemoteDataService) extends Runnable {

    var keepRunning: AtomicBoolean = new AtomicBoolean(true)
    var stopped = false

    val MAX_SIZE_ALLOWED = 0
    val CLEAN_INTERVAL = 20000
    val CACHE_DIR_PATH: Path = Paths.get(rds.getExternalStorageAttributes.cacheDir)

    override def run(): Unit = {
      var cntr = 0
      while (keepRunning.get()) {
        val files = getFileNamesToDelete
        if (files.nonEmpty) {
          rds.cleanerLock.writeLock().lock()
          try {
            // Just rename first so that write lock holding time is less
            // and then delete later after releasing the write lock
            files.foreach(orig => {
              val renamedFile = Paths.get(
                s"$CACHE_DIR_PATH/.$cntr.${RemoteDataService.TRASH_FILE_EXT}")
              cntr += 1
              rds.entryCache.invalidate(orig.getFileName)
              Files.move(orig, renamedFile, StandardCopyOption.REPLACE_EXISTING)
            })
          } finally {
            rds.cleanerLock.writeLock().unlock()
          }
        }
        // Do actual delete after releasing the write lock
        Files.list(CACHE_DIR_PATH).filter(
          TrashFilePredicate).toArray().foreach( o => Files.delete(o.asInstanceOf[Path]))
        Thread.sleep(CLEAN_INTERVAL)
      }
      stopped = true
    }

    object CacheFilePredicate extends Predicate[Path] {
      val END_WITH_EXT_PATH: Path = Paths.get(RemoteDataService.CACHED_VALUE_FILE_EXT)

      override def test(t: Path): Boolean = {
        t.endsWith(END_WITH_EXT_PATH)
      }
    }

    object TrashFilePredicate extends Predicate[Path] {
      val TRASH_EXT_PATH: Path = Paths.get(RemoteDataService.TRASH_FILE_EXT)
      override def test(t: Path): Boolean = {
        t.endsWith(TRASH_EXT_PATH)
      }
    }

    object SizeFunction extends ToLongFunction[Path] {
      override def applyAsLong(p: Path): Long = Files.size(p)
    }

    object FileAccessTimeComparator extends Comparator[Path] {
      override def compare(o1: Path, o2: Path): Int = {
        val bFA1 = Files.readAttributes(o1, classOf[BasicFileAttributes])
        val bFA2 = Files.readAttributes(o2, classOf[BasicFileAttributes])
        val diff = bFA1.lastAccessTime().toMillis - bFA2.lastAccessTime().toMillis
        if ( diff <=0 ) return -1
        1
      }
    }

    def getFileNamesToDelete: Seq[Path] = {
      val allFiles = Files.list(CACHE_DIR_PATH).filter(CacheFilePredicate)
      val currentSize = allFiles.mapToLong(SizeFunction).sum()
      val bytesToDelete = currentSize - MAX_SIZE_ALLOWED
      logDebug(s"Bytes to delete is $bytesToDelete")
      // TODO Check if the array needs to be reversed
      val sortedInAccessTime = allFiles.sorted(FileAccessTimeComparator).toArray
      var remainingBytesToDelete = bytesToDelete
      val filesToDelete = new mutable.ListBuffer[Path]
      var idx = 0
      while ( idx < sortedInAccessTime.length && remainingBytesToDelete > 0) {
        val path = sortedInAccessTime(idx).asInstanceOf[Path]
        remainingBytesToDelete = remainingBytesToDelete - Files.size(path)
        idx += 1
        filesToDelete += path
      }
      filesToDelete
    }

    def stop(): Unit = {
      keepRunning.set(false)
    }
  }

  // TODO implement
  def stop(): Unit = {
    // Take a write lock on the cleanerLock which will mean all iterators
    // are done and then wait for the cleaner to stop.
    cleanerLock.writeLock().lock()
    try {
      cleaner.stop()
      while (!cleaner.stopped) Thread.sleep(200)
    } finally {
      cleanerLock.writeLock().unlock()
    }
  }
}

case class ValueObject(filename: String, ptr: DiskEntry, rds: RemoteDataService)

object RemoteDataService extends Logging {

  // pfr = pulled from remote
  val CACHED_VALUE_FILE_EXT = ".pfr"
  val TRASH_FILE_EXT = ".trash"

  val remoteServices = mutable.Map[ExtendedStorageAttributes, RemoteDataService]()

  def apply(attrs: ExtendedStorageAttributes,
    cache: GemFireCacheImpl): RemoteDataService = synchronized {
    if (remoteServices.contains(attrs)) {
      log.warn(s"Remote service for Disk Store ${attrs.ds} already initialized")
      throw new RuntimeException(s"Remote service for Disk Store ${attrs.ds} already initialized")
    }
    val svc = new RemoteDataService(attrs, cache)
    logDebug(s"Created a remote service for Disk Store ${attrs.ds}")
    remoteServices += (attrs -> svc)
    svc
  }

  def getFileName(ds: DiskStoreImpl, ptr: DiskEntry): String = {
    val did = ptr.getDiskId
    val sb: StringBuilder = new mutable.StringBuilder()
    sb.append(ds.getName)
    sb.append("-")
    sb.append(did.getOplogId)
    sb.append("-")
    sb.append(did.getKeyId)
    sb.append("-")
    sb.append(did.getOffsetInOplog)
    sb.append("-")
    sb.append(did.getValueLength)
    sb.append(CACHED_VALUE_FILE_EXT)
    sb.toString()
  }
}