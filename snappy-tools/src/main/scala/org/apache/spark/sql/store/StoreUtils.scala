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
package org.apache.spark.sql.store

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.scheduler.SparkListenerUnpersistRDD
import org.apache.spark.sql.collection.{MultiExecutorLocalPartition, Utils}
import org.apache.spark.sql.columntable.StoreCallbacksImpl
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.storage.{BlockManagerId, RDDInfo, StorageLevel}
import org.apache.spark.{Logging, Partition, SparkContext}

/*/10/15.
  */
object StoreUtils extends Logging {

  val PARTITION_BY = ExternalStoreUtils.PARTITION_BY
  val REPLICATE = ExternalStoreUtils.REPLICATE
  val BUCKETS = ExternalStoreUtils.BUCKETS
  val COLOCATE_WITH = "COLOCATE_WITH"
  val REDUNDANCY = "REDUNDANCY"
  val RECOVERYDELAY = "RECOVERYDELAY"
  val MAXPARTSIZE = "MAXPARTSIZE"
  val EVICTION_BY = "EVICTION_BY"
  val PERSISTENT = "PERSISTENT"
  val SERVER_GROUPS = "SERVER_GROUPS"
  val OFFHEAP = "OFFHEAP"
  val EXPIRE = "EXPIRE"
  val OVERFLOW = "OVERFLOW"

  val GEM_PARTITION_BY = "PARTITION BY"
  val GEM_BUCKETS = "BUCKETS"
  val GEM_COLOCATE_WITH = "COLOCATE WITH"
  val GEM_REDUNDANCY = "REDUNDANCY"
  val GEM_REPLICATE = "REPLICATE"
  val GEM_RECOVERYDELAY = "RECOVERYDELAY"
  val GEM_MAXPARTSIZE = "MAXPARTSIZE"
  val GEM_EVICTION_BY = "EVICTION BY"
  val GEM_PERSISTENT = "PERSISTENT"
  val GEM_SERVER_GROUPS = "SERVER GROUPS"
  val GEM_OFFHEAP = "OFFHEAP"
  val GEM_EXPIRE = "EXPIRE"
  val GEM_OVERFLOW = "EVICTACTION OVERFLOW"
  val GEM_HEAPPERCENT = "EVICTION BY LRUHEAPPERCENT "
  val PRIMARY_KEY = "PRIMARY KEY"
  val LRUCOUNT = "LRUCOUNT"

  val ddlOptions = Seq(PARTITION_BY, REPLICATE, BUCKETS, COLOCATE_WITH,
    REDUNDANCY, RECOVERYDELAY, MAXPARTSIZE, EVICTION_BY,
    PERSISTENT, SERVER_GROUPS, OFFHEAP, EXPIRE, OVERFLOW)

  val EMPTY_STRING = ""
  val NONE = "NONE"

  val SHADOW_COLUMN_NAME = "rowid"

  val SHADOW_COLUMN = s"$SHADOW_COLUMN_NAME bigint generated always as identity"


  def lookupName(tableName: String, schema: String): String = {
    val lookupName = {
      if (tableName.indexOf('.') <= 0) {
        schema + '.' + tableName
      } else tableName
    }
    lookupName
  }

  def getPartitionsPartitionedTable(sc: SparkContext,
      tableName: String, schema: String,
      blockMap: Map[InternalDistributedMember, BlockManagerId]): Array[Partition] = {

    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val numPartitions = region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    for (p <- 0 until numPartitions) {
      val distMembers = region.getRegionAdvisor.getBucketOwners(p).asScala
      val prefNodes = distMembers.map(
        m => blockMap.get(m)
      )
      val prefNodeSeq = prefNodes.map(_.get).toSeq
      partitions(p) = new MultiExecutorLocalPartition(p, prefNodeSeq)
    }
    partitions
  }

  def getPartitionsReplicatedTable(sc: SparkContext,
      tableName: String, schema: String,
      blockMap: Map[InternalDistributedMember, BlockManagerId]): Array[Partition] = {

    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[DistributedRegion]
    val numPartitions = 1
    val partitions = new Array[Partition](numPartitions)

    val regionMembers = if (Utils.isLoner(sc)) {
      Set(Misc.getGemFireCache.getDistributedSystem.getDistributedMember)
    } else {
      region.getDistributionAdvisor.adviseInitializedReplicates().asScala
    }
    val prefNodes = regionMembers.map(v => blockMap(v)).toSeq
    partitions(0) = new MultiExecutorLocalPartition(0, prefNodes)
    partitions
  }

  def initStore(sqlContext: SQLContext,
      table: String,
      schema: Option[StructType],
      partitions: Int,
      connProperties: ConnectionProperties): Map[InternalDistributedMember,
      BlockManagerId] = {
    // TODO for SnappyCluster manager optimize this . Rather than calling this
    val blockMap = new StoreInitRDD(sqlContext, table,
      schema, partitions, connProperties).collect()
    blockMap.toMap
  }

  def registerRDDInfoForUI(sc: SparkContext, table: String,
      numPartitions: Int): Unit = {
    StoreCallbacksImpl.stores.get(table) match {
      case Some((_, _, rddId)) =>
        val rddInfo = new RDDInfo(rddId, table, numPartitions,
          StorageLevel.OFF_HEAP, Seq())
        rddInfo.numCachedPartitions = numPartitions
        sc.ui.foreach(_.storageListener.registerRDDInfo(rddInfo))
      case None => // nothing
    }
  }

  def unregisterRDDInfoForUI(sc: SparkContext, table: String,
      numPartitions: Int): Unit = {
    StoreCallbacksImpl.stores.get(table) match {
      case Some((_, _, rddId)) =>
        sc.listenerBus.post(SparkListenerUnpersistRDD(rddId))
      case None => // nothing
    }
  }

  def removeCachedObjects(sqlContext: SQLContext, table: String,
      numPartitions: Int, registerDestroy: Boolean = false): Unit = {
    ExternalStoreUtils.removeCachedObjects(sqlContext, table, registerDestroy)
    unregisterRDDInfoForUI(sqlContext.sparkContext, table, numPartitions)
    Utils.mapExecutors(sqlContext, () => {
      StoreInitRDD.tableToIdMap.remove(table)
      StoreCallbacksImpl.stores.remove(table)
      Iterator.empty
    }).count()
    StoreInitRDD.tableToIdMap.remove(table)
    StoreCallbacksImpl.stores.remove(table)
  }

  def appendClause(sb: mutable.StringBuilder, getClause: () => String): Unit = {
    val clause = getClause.apply()
    if (!clause.isEmpty) {
      sb.append(s"$clause ")
    }
  }

  def getPrimaryKeyClause(parameters: mutable.Map[String, String]): String = {
    val sb = new StringBuilder()
    sb.append(parameters.get(PARTITION_BY).map(v => {
      val primaryKey = {
        v match {
          case PRIMARY_KEY => ""
          case _ => s"$PRIMARY_KEY ($SHADOW_COLUMN_NAME)"
        }
      }
      primaryKey
    }).getOrElse(s"$PRIMARY_KEY ($SHADOW_COLUMN_NAME)"))
    sb.toString()
  }

  def ddlExtensionString(parameters: mutable.Map[String, String],
      isRowTable: Boolean, isShadowTable: Boolean): String = {
    val sb = new StringBuilder()

    if (!isShadowTable) {
      sb.append(parameters.remove(PARTITION_BY).map(v => {
        val (parClause) = {
          v match {
            case PRIMARY_KEY =>
              if (isRowTable) {
                PRIMARY_KEY
              } else {
                throw new DDLException("Column table cannot be partitioned on" +
                    " PRIMARY KEY as no primary key")
              }
            case _ =>  s"hash COLUMN($v)"
          }
        }
        s"$GEM_PARTITION_BY $parClause "
      }
      ).getOrElse(if (isRowTable) EMPTY_STRING
      else s"$GEM_PARTITION_BY COLUMN ($SHADOW_COLUMN_NAME) "))
    } else {
      parameters.remove(PARTITION_BY).foreach {
        case PRIMARY_KEY => throw new DDLException("Column table cannot be " +
            "partitioned on PRIMARY KEY as no primary key")
        case _ =>
      }
    }

    if (!isShadowTable) {
      sb.append(parameters.remove(COLOCATE_WITH).map(v => s"$GEM_COLOCATE_WITH ($v) ")
          .getOrElse(EMPTY_STRING))
    }

    parameters.remove(REPLICATE).foreach(v =>
      if (v.toBoolean) sb.append(GEM_REPLICATE).append(' ')
      else if (!parameters.contains(BUCKETS))
        sb.append(GEM_BUCKETS).append(' ').append(
          ExternalStoreUtils.DEFAULT_TABLE_BUCKETS).append(' '))
    sb.append(parameters.remove(BUCKETS).map(v => s"$GEM_BUCKETS $v ")
        .getOrElse(EMPTY_STRING))

    sb.append(parameters.remove(REDUNDANCY).map(v => s"$GEM_REDUNDANCY $v ")
        .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(RECOVERYDELAY).map(v => s"$GEM_RECOVERYDELAY $v ")
        .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(MAXPARTSIZE).map(v => s"$GEM_MAXPARTSIZE $v ")
        .getOrElse(EMPTY_STRING))

    // if OVERFLOW has been provided, then use HEAPPERCENT as the default
    // eviction policy (unless overridden explicitly)
    val hasOverflow = parameters.get(OVERFLOW).map(_.toBoolean)
        .getOrElse(!isRowTable && !parameters.contains(EVICTION_BY))
    val defaultEviction = if (hasOverflow) GEM_HEAPPERCENT else EMPTY_STRING
    if (!isShadowTable) {
      sb.append(parameters.remove(EVICTION_BY).map(v =>
        if (v == NONE) EMPTY_STRING else s"$GEM_EVICTION_BY $v ")
          .getOrElse(defaultEviction))
    } else {
      sb.append(parameters.remove(EVICTION_BY).map(v => {
        if (v.contains(LRUCOUNT)) {
          throw new DDLException(
            "Column table cannot take LRUCOUNT as eviction policy")
        } else if (v == NONE) {
          EMPTY_STRING
        } else {
          s"$GEM_EVICTION_BY $v "
        }
      }).getOrElse(defaultEviction))
    }

    if (hasOverflow) {
      parameters.remove(OVERFLOW)
      sb.append(s"$GEM_OVERFLOW ")
    }

    parameters.remove(PERSISTENT).foreach { v =>
      if (v.equalsIgnoreCase("async") || v.equalsIgnoreCase("true")) {
        sb.append(s"$GEM_PERSISTENT ASYNCHRONOUS ")
      } else if (v.equalsIgnoreCase("sync")) {
        sb.append(s"$GEM_PERSISTENT SYNCHRONOUS ")
      } else if (!v.equalsIgnoreCase("false")) {
        sb.append(s"$GEM_PERSISTENT $v ")
      }
    }
    sb.append(parameters.remove(SERVER_GROUPS).map(v => s"$GEM_SERVER_GROUPS $v ")
        .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(OFFHEAP).map(v => s"$GEM_OFFHEAP $v ")
        .getOrElse(EMPTY_STRING))


    sb.append(parameters.remove(EXPIRE).map(v => {
      if (!isRowTable) {
        throw new DDLException("Expiry for Column table is not supported")
      }
      s"$GEM_EXPIRE ENTRY WITH TIMETOLIVE $v ACTION DESTROY"
    }).getOrElse(EMPTY_STRING))

    sb.toString()
  }

  def getPartitioningColumn(parameters: mutable.Map[String, String]): Seq[String] = {
    parameters.get(PARTITION_BY).map(v => {
      v.split(",").toSeq.map(a => a.trim)
    }).getOrElse(Seq.empty[String])
  }

  def validateConnProps(parameters: mutable.Map[String, String]): Unit = {
    parameters.keys.forall(v => {
      if (!ddlOptions.contains(v.toString.toUpperCase)) {
        throw new AnalysisException(s"Unknown options $v specified while creating table ")
      }
      true
    })
  }
}
