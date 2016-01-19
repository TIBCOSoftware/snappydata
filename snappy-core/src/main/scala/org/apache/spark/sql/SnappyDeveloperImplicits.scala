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

package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.runtime.{universe => u}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.{ExternalStoreRelation, CachedBatch, InMemoryAppendableRelation}
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.snappy._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/**
 * Developer and experimental APIs on SnappyContext. Many of them might be removed.
 *
 * Don't use them unless you know what you are doing.
 */

object devapi {

  implicit def snappyContextDevApi(snc: SnappyContext): SnappyContextDevOperations = {
    SnappyContextDevOperations(snc)
  }
}

private[sql] case class SnappyContextDevOperations(context: SnappyContext) {

  /**
   * Append dataframe to cache table in Spark.
   * @todo should this be renamed to appendToTempTable(...) ?
   *
   * @param df
   * @param table
   * @param storageLevel default storage level is MEMORY_AND_DISK
   * @return  @todo -> return type?
   */
  def appendToCache(df: DataFrame, table: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
    val useCompression = context.conf.useCompression
    val columnBatchSize = context.conf.columnBatchSize

    val tableIdent = context.catalog.newQualifiedTableName(table)
    val plan = context.catalog.lookupRelation(tableIdent, None)
    val relation = context.cacheManager.lookupCachedData(plan).getOrElse {
      context.cacheManager.cacheQuery(DataFrame(context, plan),
        Some(tableIdent.table), storageLevel)

      context.cacheManager.lookupCachedData(plan).getOrElse {
        sys.error(s"couldn't cache table $tableIdent")
      }
    }

    val (schema, output) = (df.schema, df.logicalPlan.output)

    val cached = df.rdd.mapPartitionsPreserve { rowIterator =>

      val batches = ExternalStoreRelation(useCompression, columnBatchSize,
        tableIdent, schema, relation.cachedRepresentation, output)

      val converter = CatalystTypeConverters.createToCatalystConverter(schema)

      rowIterator.map(converter(_).asInstanceOf[InternalRow])
          .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator
    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation match {
        case externalStore: ExternalStoreRelation =>
          externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
        case appendable: InMemoryAppendableRelation =>
          appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
      }
    }
  }
  /**
   * @param rdd
   * @param table
   * @param schema
   * @param storageLevel
   */
  private[sql] def appendToCacheRDD(rdd: RDD[_], table: String, schema:
  StructType,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = context.conf.useCompression
    val columnBatchSize = context.conf.columnBatchSize

    val tableIdent = context.catalog.newQualifiedTableName(table)
    val plan = context.catalog.lookupRelation(tableIdent, None)
    val relation = context.cacheManager.lookupCachedData(plan).getOrElse {
      context.cacheManager.cacheQuery(DataFrame(context, plan),
        Some(tableIdent.table), storageLevel)

      context.cacheManager.lookupCachedData(plan).getOrElse {
        sys.error(s"couldn't cache table $tableIdent")
      }
    }

    val cached = rdd.mapPartitionsPreserve { rowIterator =>

      val batches = ExternalStoreRelation(useCompression, columnBatchSize,
        tableIdent, schema, relation.cachedRepresentation, schema.toAttributes)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowIterator.map(converter(_).asInstanceOf[InternalRow])
          .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator

    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation match {
        case externalStore: ExternalStoreRelation =>
          externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
        case appendable: InMemoryAppendableRelation =>
          appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
      }
    }
  }

  /**
   * :: DeveloperApi ::
   * @todo do we need this anymore? If useful functionality, make this
   *       private to sql package ... SchemaDStream should use the data source
   *       API?
   *       Tagging as developer API, for now
   * @param stream
   * @param aqpTables
   * @param transformer
   * @param v
   * @tparam T
   * @return
   */
  @DeveloperApi
  def saveStream[T](stream: DStream[T],
      aqpTables: Seq[String],
      transformer: Option[(RDD[T]) => RDD[Row]])(implicit v: u.TypeTag[T]) {
    val transfrmr = transformer match {
      case Some(x) => x
      case None => if (!(v.tpe =:= u.typeOf[Row])) {
        //check if the stream type is already a Row
        throw new IllegalStateException(" Transformation to Row type needs to be supplied")
      } else {
        null
      }
    }
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val rddRows = if (transfrmr != null) {
        transfrmr(rdd)
      } else {
        rdd.asInstanceOf[RDD[Row]]
      }
      context.snappyContextFunctions.collectSamples(context, rddRows, aqpTables, time.milliseconds)
    })
  }

  /**
   * @todo remove its reference from SnappyImplicits ... use DataSource API
   *       instead
   * @param df
   * @param aqpTables
   */
  def saveTable(df: DataFrame, aqpTables: Seq[String]): Unit = context
      .snappyContextFunctions.collectSamples(context, df.rdd,
    aqpTables, System.currentTimeMillis())

  /**
   * :: DeveloperApi ::
   * @todo Remove ... use DataSource API
   * @param tableName
   * @param schema
   * @param samplingOptions
   * @param streamTable
   * @param jdbcSource
   * @return
   */
  @DeveloperApi
  def registerSampleTable(tableName: String, schema: StructType,
      samplingOptions: Map[String, Any], streamTable: Option[String] = None,
      jdbcSource: Option[Map[String, String]] = None): SampleDataFrame =
    context.snappyContextFunctions.registerSampleTable(context, tableName, schema,
      samplingOptions, streamTable,
      jdbcSource)

  /**
   * :: DeveloperApi ::
   * @todo why can't this be done using dropTable ?
   */
  @DeveloperApi
  def dropSampleTable(tableName: String, ifExists: Boolean = false): Unit = {

    val qualifiedTable = context.catalog.newQualifiedTableName(tableName)
    val plan = try {
      context.catalog.lookupRelation(qualifiedTable, None)
    } catch {
      case e@(_: AnalysisException | _: DDLException) =>
        if (ifExists) return else throw e
    }
    context.cacheManager.tryUncacheQuery(DataFrame(context, plan))
    context.catalog.unregisterTable(qualifiedTable)
    context.snappyContextFunctions.dropSampleTable(tableName, ifExists)
  }
}

