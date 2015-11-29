package org.apache.spark.sql.columntable


import java.util
import java.util.{Collections, UUID}

import com.gemstone.gemfire.internal.cache.{BucketRegion, PartitionedRegion}
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, StoreCallbacks}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer}
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.{ScanController, TransactionController}
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types._

import scala.collection.{JavaConversions, mutable}

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
        case Some((previousSchema, _)) => if (previousSchema != schema) stores.put(tableName, (schema, externalStore))
      }
    }
    sqlContext = Some(context)
    useCompression = compress
    cachedBatchSize = batchSize
  }

  def createCachedBatchWithConn(region: BucketRegion, batchID: UUID, bucketID: Int) = {
    val container: GemFireContainer = region.getPartitionedRegion.getUserAttribute.asInstanceOf[GemFireContainer]
    val tableName = container.getTableName
    if (stores.get(tableName) != None) {
      val (schema, externalStore) = stores.get(container.getTableName).get

      val conn = externalStore.getConnection(container.getTableName)

      Misc.getRegionForTable("APP.COLUMNTABLE4", true).asInstanceOf[PartitionedRegion]
      val ps1 = conn.prepareStatement(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION($tableName, $bucketID)")
      ps1.execute()
      val ps2 = conn.prepareStatement(s"SELECT * FROM $tableName")
      val rs = ps2.executeQuery()
      val batchCreator = new CachedBatchCreator(s"${container.getTableName}_SHADOW_", schema,
             externalStore, cachedBatchSize, useCompression)

      batchCreator.createAndStoreBatchFromRS(rs, batchID, bucketID)
    }
  }

  override def createCachedBatch(region: BucketRegion, batchID: UUID, bucketID: Int) : java.util.Set[Any] = {
    val container: GemFireContainer = region.getPartitionedRegion.getUserAttribute.asInstanceOf[GemFireContainer]

    println("ABCD creating cached batch for batchID  " + batchID)

    if (stores.get(container.getTableName) != None) {
      val (schema, externalStore) = stores.get(container.getTableName).get
      //LCC should be available assuming insert is already being done via a proper connection
      var conn: EmbedConnection = null
      var contextSet: Boolean = false
      try {
        var lcc: LanguageConnectionContext = Misc.getLanguageConnectionContext()
        if (lcc == null) {
          conn = GemFireXDUtils.getTSSConnection(true, true, false)
          conn.getTR.setupContextStack
          contextSet = true
          lcc = conn.getLanguageConnectionContext
          if (lcc == null) {
            Misc.getGemFireCache.getCancelCriterion.checkCancelInProgress(null)
          }
        }
        val row: AbstractCompactExecRow = container.newTemplateRow().asInstanceOf[AbstractCompactExecRow]
        lcc.setExecuteLocally(Collections.singleton(bucketID), region.getPartitionedRegion, false, null);
        try {
          val sc: ScanController = lcc.getTransactionExecute().openScan(container.getId().getContainerId(),
            false, 0, TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_NOLOCK /* not used */ , null, null, 0, null, null, 0, null);

          val batchCreator = new CachedBatchCreator(s"${container.getTableName}_SHADOW_", schema,
            externalStore, cachedBatchSize, useCompression)
          val keys = batchCreator.createAndStoreBatch(sc, row, batchID, bucketID)
          JavaConversions.mutableSetAsJavaSet(keys)
        }
        finally {
          lcc.setExecuteLocally(null, null, false, null);
        }
      }
      catch {
        case e : Throwable => throw e
      } finally {
        if (contextSet) {
          conn.getTR.restoreContextStack
        }
      }
    } else {
      new util.HashSet()
    }
  }
}

trait StoreCallback extends Serializable {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}
