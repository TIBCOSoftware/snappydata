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
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicLong

import com.gemstone.gemfire.internal.cache.{DiskEntry, DiskStoreImpl}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.Logging

import scala.collection.mutable

case class ExtendedStorageAttributes(ds: DiskStoreImpl,
  extType: String, cacheDir: String, maxCacheSizeInMB: Long)

final class RemoteDataService(extStorageAttrs: ExtendedStorageAttributes) extends Logging {

  val ds: DiskStoreImpl = extStorageAttrs.ds
  // TODO: Better toString
  override def toString: String = s"${extStorageAttrs.extType}"

  private[this] lazy val entryCache = {
    // Keeping the cache size very big. Actual data cached will depend on the amount
    // of disk space configured for keeping the cached data.
    CacheBuilder.newBuilder().maximumSize(10000000).build[DiskEntry, String]()
  }

  lazy val client: RemoteDataClient = null

  var currentCacheSize: AtomicLong = calculateInitialSizeUsed()

  def calculateInitialSizeUsed(): AtomicLong = {
    var sz = 0
    var count = 0
    val cacheDir = new File(extStorageAttrs.cacheDir)
    for (file <- cacheDir.listFiles) {
      // TODO: change when exact file pattern name is decided
      if (file.isFile && file.getName.contains("oplog")) {
        sz += file.length
        count += 1
      }
    }
    logDebug(s"cache directory has $count files of total size $sz for $this")
    new AtomicLong(sz)
  }

  // TODO: Remote Simultaneous access?? We can synchronize on filename or how?
  def getValues(diskptrs: Array[DiskEntry]): Array[Array[Byte]] = diskptrs.map(ptr => {
    val filename = RemoteDataService.getFileName(ds, ptr)
    var cachedValFile = entryCache.getIfPresent(ptr)
    var bytes: Array[Byte] = null
    val len = ptr.getDiskId.getValueLength
    if ( cachedValFile == null ) {
      val offSet = ptr.getDiskId.getOffsetInOplog
      val range = Array[Long](offSet, offSet + len)
      bytes = client.getValues(filename, range)
      // Write the bytes in the file and cache and update the cache
      val bos = new BufferedOutputStream(new FileOutputStream(filename))
      bos.write(bytes)
      bos.close()
      entryCache.put(ptr, filename)
      currentCacheSize.addAndGet(bytes.size)
    } else {
      bytes = Files.readAllBytes(Paths.get(filename))
    }
    bytes
  })
}

object RemoteDataService extends Logging {

  val remoteServices: mutable.Map[ExtendedStorageAttributes, RemoteDataService] =
    new mutable.HashMap[ExtendedStorageAttributes, RemoteDataService]()

  def apply(attrs: ExtendedStorageAttributes): RemoteDataService = {
    synchronized(RemoteDataService.getClass) {
      if (remoteServices.contains(attrs)) {
        log.warn(s"Remote service for Disk Store ${attrs.ds} already initialized")
        throw new RuntimeException(s"Remote service for Disk Store ${attrs.ds} already initialized")
      }
      val svc = new RemoteDataService(attrs)
      logDebug(s"Created a remote service for Disk Store ${attrs.ds}")
      remoteServices += (attrs -> svc)
    }
  }

  def getFileName(ds: DiskStoreImpl, ptr: DiskEntry): String = {
    null
  }
}
