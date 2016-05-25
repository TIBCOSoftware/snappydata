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
package org.apache.spark.sql.execution.columnar.impl

import java.util.{Collections, UUID}

import scala.collection.JavaConversions
import scala.collection.concurrent.TrieMap

import com.gemstone.gemfire.internal.cache.BucketRegion
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, StoreCallbacks}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer}
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.{ScanController, TransactionController}
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import io.snappydata.Constant

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.columnar.{CachedBatchCreator, ExternalStore}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.store.StoreHashFunction
import org.apache.spark.sql.types._

object StoreCallbacksImpl extends StoreCallbacks with Logging with Serializable {

  @transient private var sqlContext = None: Option[SQLContext]
  val stores = new TrieMap[String, (StructType, ExternalStore)]

  val partioner = new StoreHashFunction


  var useCompression = false
  var cachedBatchSize = 0

  def registerExternalStoreAndSchema(context: SQLContext, tableName: String,
      schema: StructType, externalStore: ExternalStore,
      batchSize: Int, compress: Boolean): Unit = {
    stores.synchronized {
      stores.get(tableName) match {
        case None => stores.put(tableName, (schema, externalStore))
        case Some((previousSchema, _)) =>
          if (previousSchema != schema) {
            stores.put(tableName, (schema, externalStore))
          }
      }
    }
    sqlContext = Some(context)
    useCompression = compress
    cachedBatchSize = batchSize
  }

  override def createCachedBatch(region: BucketRegion, batchID: UUID,
      bucketID: Int): java.util.Set[Any] = {
    val container: GemFireContainer = region.getPartitionedRegion
        .getUserAttribute.asInstanceOf[GemFireContainer]
    val store = stores.get(container.getTableName)
    if (store.isDefined) {
      val (schema, externalStore) = store.get
      // LCC should be available assuming insert is already being done
      // via a proper connection
      var conn: EmbedConnection = null
      var contextSet: Boolean = false
      try {
        var lcc: LanguageConnectionContext = Misc.getLanguageConnectionContext
        if (lcc == null) {
          conn = GemFireXDUtils.getTSSConnection(true, true, false)
          conn.getTR.setupContextStack()
          contextSet = true
          lcc = conn.getLanguageConnectionContext
          if (lcc == null) {
            Misc.getGemFireCache.getCancelCriterion.checkCancelInProgress(null)
          }
        }
        val row: AbstractCompactExecRow = container.newTemplateRow()
            .asInstanceOf[AbstractCompactExecRow]
        lcc.setExecuteLocally(Collections.singleton(bucketID),
          region.getPartitionedRegion, false, null)
        try {
          val sc: ScanController = lcc.getTransactionExecute.openScan(
            container.getId.getContainerId, false, 0,
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_NOLOCK /* not used */ ,
            null, null, 0, null, null, 0, null)

          val batchCreator = new CachedBatchCreator(
            ColumnFormatRelation.cachedBatchTableName(container.getTableName),
            container.getTableName, schema,
            externalStore, cachedBatchSize, useCompression)
          val keys = batchCreator.createAndStoreBatch(sc, row,
            batchID, bucketID)
          JavaConversions.mutableSetAsJavaSet(keys)
        }
        finally {
          lcc.setExecuteLocally(null, null, false, null)
        }
      }
      catch {
        case e: Throwable => throw e
      } finally {
        if (contextSet) {
          conn.getTR.restoreContextStack()
        }
      }
    } else {
      new java.util.HashSet()
    }
  }

  def getInternalTableSchemas: java.util.List[String] = {
    val schemas = new java.util.ArrayList[String](2)
    schemas.add(SnappyStoreHiveCatalog.HIVE_METASTORE)
    schemas.add(Constant.INTERNAL_SCHEMA_NAME)
    schemas
  }

  override def getHashCodeSnappy(dvd: scala.Any): Int = {
    partioner.hashValue(dvd)
  }

  override def getHashCodeSnappy(dvds: scala.Array[Object]): Int = {
    partioner.hashValue(dvds)
  }

  override def haveRegisteredExternalStore(tableName: String): Boolean = {
    // TODO -  remove below that deals with default schema and all
    // entries in store should come with fully qualified tableName
    val table = if (tableName.startsWith(Constant.DEFAULT_SCHEMA + ".")) {
      tableName.substring(Constant.DEFAULT_SCHEMA.length + 1)
    } else tableName
    stores.contains(table)
  }

}

trait StoreCallback extends Serializable {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}
