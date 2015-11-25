package org.apache.spark.sql.columntable


import java.util.{Collections, UUID}

import org.apache.spark.sql.SQLContext

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, BucketRegion}
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, StoreCallbacks}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer}
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.{ScanController, TransactionController}

import org.apache.spark.Logging
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types._

/**
 * Created by skumar on 6/11/15.
 */
object StoreCallbacksImpl extends StoreCallbacks with Logging with Serializable {

  @transient private var sqlContext = None: Option[SQLContext]
  val stores = new mutable.HashMap[String, (StructType, ExternalStore)]

  var useCompression = false
  var cachedBatchSize = 0

  def registerExternalStoreAndSchema(context: SQLContext, tableName: String,
      schema: StructType, externalStore: ExternalStore, batchSize: Int, compress : Boolean) = {
    stores.synchronized {
      stores.get(tableName) match {
        case None => stores.put(tableName, (schema, externalStore))
        case Some(v) =>
      }
    }
    sqlContext = Some(context)
    useCompression = compress
    cachedBatchSize = batchSize
  }

  override def createCachedBatch(region: BucketRegion, batchID: UUID, bucketID: Int) = {
    val container: GemFireContainer = region.getPartitionedRegion.getUserAttribute.asInstanceOf[GemFireContainer]
    if (stores.get(container.getTableName) != None) {
      val (schema, externalStore) = stores.get(container.getTableName).get

      // LCC should be available assuming insert is already being done via a proper connection
      val lcc: LanguageConnectionContext = Misc.getLanguageConnectionContext()
      val row: AbstractCompactExecRow = container.newTemplateRow().asInstanceOf[AbstractCompactExecRow]
      lcc.setExecuteLocally(Collections.singleton(bucketID), region, false, null);
      try {
        val sc: ScanController = lcc.getTransactionExecute().openScan(container.getId().getContainerId(),
          false, 0, TransactionController.MODE_RECORD,
          TransactionController.ISOLATION_NOLOCK /* not used */ , null, null, 0, null, null, 0, null);

        val batchCreator = new CachedBatchCreator(s"${container.getTableName}_SHADOW_", schema,
          externalStore, cachedBatchSize, useCompression)
        batchCreator.createAndStoreBatch(sc, row, batchID, bucketID)
      }
      finally {
        lcc.setExecuteLocally(null, null, false, null);
      }
    }
  }
}

trait StoreCallback {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}
