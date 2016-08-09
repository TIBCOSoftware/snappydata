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
import scala.collection.generic.Growable
import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.sql.collection.{MultiExecutorLocalPartition, Utils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.StoreCallbacksImpl
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext, SnappyContext}
import org.apache.spark.{Logging, Partition, SparkContext}

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
  val DISKSTORE = "DISKSTORE"
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
  val GEM_INDEXED_TABLE = "INDEXED_TABLE"

  // int values for Spark SQL types for efficient switching avoiding reflection
  val STRING_TYPE = 0
  val INT_TYPE = 1
  val LONG_TYPE = 2
  val BINARY_LONG_TYPE = 3
  val SHORT_TYPE = 4
  val BYTE_TYPE = 5
  val BOOLEAN_TYPE = 6
  val DECIMAL_TYPE = 7
  val DOUBLE_TYPE = 8
  val FLOAT_TYPE = 9
  val DATE_TYPE = 10
  val TIMESTAMP_TYPE = 11
  val BINARY_TYPE = 12
  val ARRAY_TYPE = 13
  val MAP_TYPE = 14
  val STRUCT_TYPE = 15

  val ddlOptions = Seq(PARTITION_BY, REPLICATE, BUCKETS, COLOCATE_WITH,
    REDUNDANCY, RECOVERYDELAY, MAXPARTSIZE, EVICTION_BY,
    PERSISTENT, SERVER_GROUPS, OFFHEAP, EXPIRE, OVERFLOW,
    GEM_INDEXED_TABLE, ExternalStoreUtils.INDEX_NAME)

  val EMPTY_STRING = ""
  val NONE = "NONE"

  val SHADOW_COLUMN_NAME = "rowid"

  val SHADOW_COLUMN = s"$SHADOW_COLUMN_NAME bigint generated always as identity"

  def getPartitionsPartitionedTable(sc: SparkContext,
      region: PartitionedRegion): Array[Partition] = {

    val numPartitions = region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    for (p <- 0 until numPartitions) {
      val distMembers = region.getRegionAdvisor.getBucketOwners(p).asScala
      val prefNodes = distMembers.map(
        m => SnappyContext.storeToBlockMap.get(m.toString)
      ).filter(_.isDefined)
      val prefNodeSeq = prefNodes.map(_.get).toSeq
      partitions(p) = new MultiExecutorLocalPartition(p, prefNodeSeq)
    }
    partitions
  }

  def getPartitionsReplicatedTable(sc: SparkContext,
      region: CacheDistributionAdvisee): Array[Partition] = {

    val numPartitions = 1
    val partitions = new Array[Partition](numPartitions)

    val regionMembers = if (Utils.isLoner(sc)) {
      Set(Misc.getGemFireCache.getDistributedSystem.getDistributedMember)
    } else {
      region.getCacheDistributionAdvisor.adviseInitializedReplicates().asScala
    }
    val prefNodes = regionMembers.map(v => SnappyContext.storeToBlockMap(v.toString)).toSeq
    partitions(0) = new MultiExecutorLocalPartition(0, prefNodes)
    partitions
  }

  def initStore(sqlContext: SQLContext,
      table: String,
      schema: Option[StructType],
      partitions: Int,
      connProperties: ConnectionProperties): Unit = {
    // TODO for SnappyCluster manager optimize this . Rather than calling this
    new StoreInitRDD(sqlContext, table, schema, partitions, connProperties)
        .collect()
  }

  def removeCachedObjects(sqlContext: SQLContext, table: String,
      registerDestroy: Boolean = false): Unit = {
    ExternalStoreUtils.removeCachedObjects(sqlContext, table, registerDestroy)
    Utils.mapExecutors(sqlContext, () => {
      StoreCallbacksImpl.stores.remove(table)
      Iterator.empty
    }).count()
    StoreCallbacksImpl.stores.remove(table)
  }

  def appendClause(sb: mutable.StringBuilder, getClause: () => String): Unit = {
    val clause = getClause.apply()
    if (!clause.isEmpty) {
      sb.append(s"$clause ")
    }
  }

  val pkDisallowdTypes = Seq(StringType, BinaryType, ArrayType, MapType, StructType)

  def getPrimaryKeyClause(parameters: mutable.Map[String, String],
      schema: StructType,
      context: SQLContext): String = {
    val sb = new StringBuilder()
    sb.append(parameters.get(PARTITION_BY).map(v => {
      val primaryKey = {
        v match {
          case PRIMARY_KEY => ""
          case _ =>
            val normalizedSchema = context.catalog.asInstanceOf[SnappyStoreHiveCatalog]
                .normalizeSchema(schema)
            val schemaFields = Utils.schemaFields(normalizedSchema)
            val cols = v.split(",") map (_.trim)
            val normalizedCols = cols map { c =>
              if (context.conf.caseSensitiveAnalysis) {
                c
              } else {
                if (Utils.hasLowerCase(c)) Utils.toUpperCase(c) else c
              }
            }
            val prunedSchema = ExternalStoreUtils.pruneSchema(schemaFields, normalizedCols)

            val b = for (field <- prunedSchema.fields)
              yield !pkDisallowdTypes.contains(field.dataType)

            val includeInPK = b.forall(identity)
            if (includeInPK) {
              s"$PRIMARY_KEY ($v, $SHADOW_COLUMN_NAME)"
            } else {
              s"$PRIMARY_KEY ($SHADOW_COLUMN_NAME)"
            }
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
                s"sparkhash $PRIMARY_KEY"
              } else {
                throw new DDLException("Column table cannot be partitioned on" +
                  " PRIMARY KEY as no primary key")
              }
            case _ => s"sparkhash COLUMN($v)"
          }
        }
        s"$GEM_PARTITION_BY $parClause "
      }).getOrElse(if (isRowTable) EMPTY_STRING
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
      else if (!parameters.contains(BUCKETS)) sb.append(GEM_BUCKETS).append(' ')
        .append(ExternalStoreUtils.DEFAULT_TABLE_BUCKETS).append(' '))
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

    var isPersistent = false
    parameters.remove(PERSISTENT).foreach { v =>
      if (v.equalsIgnoreCase("async") || v.equalsIgnoreCase("asynchronous")) {
        sb.append(s"$GEM_PERSISTENT ASYNCHRONOUS ")
        isPersistent = true
      } else if (v.equalsIgnoreCase("sync") ||
          v.equalsIgnoreCase("synchronous")) {
        sb.append(s"$GEM_PERSISTENT SYNCHRONOUS ")
        isPersistent = true
      } else {
        throw new DDLException(s"Invalid value for option $PERSISTENT = $v" +
          s" (expected one of: async, sync, asynchronous, synchronous)")
      }
    }
    parameters.remove(DISKSTORE).foreach { v =>
      if (isPersistent) sb.append(s"'$v' ")
      else throw new DDLException(
        s"Option '$DISKSTORE' requires '$PERSISTENT' option")
    }
    sb.append(parameters.remove(SERVER_GROUPS)
      .map(v => s"$GEM_SERVER_GROUPS ($v) ")
      .getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(OFFHEAP).map(v =>
      if (v.equalsIgnoreCase("true")) s"$GEM_OFFHEAP " else EMPTY_STRING)
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
        throw new AnalysisException(
          s"Unknown options $v specified while creating table ")
      }
      true
    })
  }

  def mapCatalystTypes(schema: StructType,
      types: Growable[DataType]): Array[Int] = {
    var i = 0
    val result = new Array[Int](schema.length)
    while (i < schema.length) {
      val field = schema.fields(i)
      val dataType = field.dataType
      if (types != null) {
        types += dataType
      }
      result(i) = dataType match {
        case StringType => STRING_TYPE
        case IntegerType => INT_TYPE
        case LongType =>
          if (field.metadata.contains("binarylong")) BINARY_LONG_TYPE
          else LONG_TYPE
        case ShortType => SHORT_TYPE
        case ByteType => BYTE_TYPE
        case BooleanType => BOOLEAN_TYPE
        case _: DecimalType => DECIMAL_TYPE
        case DoubleType => DOUBLE_TYPE
        case FloatType => FLOAT_TYPE
        case DateType => DATE_TYPE
        case TimestampType => TIMESTAMP_TYPE
        case BinaryType => BINARY_TYPE
        case _: ArrayType => ARRAY_TYPE
        case _: MapType => MAP_TYPE
        case _: StructType => STRUCT_TYPE
        case _ => throw new IllegalArgumentException(
          s"Unsupported field $field")
      }
      i += 1
    }
    result
  }
}
