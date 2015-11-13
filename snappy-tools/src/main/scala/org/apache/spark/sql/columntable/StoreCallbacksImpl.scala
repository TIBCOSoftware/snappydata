package org.apache.spark.sql.columntable


import java.util
import java.util.UUID

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{RegionEntry, BucketRegion}
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, StoreCallbacks}
import com.pivotal.gemfirexd.internal.engine.store.{RawStoreResultSet, GemFireContainer}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.CachedBatchCreator
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types.StructType

/**
 * Created by skumar on 6/11/15.
 */
object StoreCallbacksImpl extends StoreCallbacks with Logging {

  var stores = new mutable.HashMap[String, (StructType, ExternalStore)]

  def registerExternalStoreAndSchema(tableName: String, schema: StructType, externalStore: ExternalStore) = {
    stores.get(tableName) match {
      case None => stores.put(tableName, (schema, externalStore))
      case Some(v) =>
    }
  }

  override def createCachedBatch(region: BucketRegion, batchID: UUID, bucketID: Int) = {

    val container: GemFireContainer = region.getPartitionedRegion.getUserAttribute.asInstanceOf[GemFireContainer]
    val itr: util.Iterator[RegionEntry] = region.getBestLocalIterator(true)

    if (stores.get(container.getTableName) != None) {
      val (schema, externalStore) = stores.get(container.getTableName).get

      val cachedBatchCreator = new CachedBatchCreator(s"${container.getTableName}_SHADOW_", schema, externalStore)

      val internalRowIterator = new Iterator[InternalRow] {
        override def next(): InternalRow = {
          val re: RegionEntry = itr.next()
          val newVal = re._getValue()
          if (!re.isDestroyedOrRemoved) {
            if (newVal.getClass eq classOf[Array[Byte]]) {
              val row: Array[Byte] = newVal.asInstanceOf[Array[Byte]]
              val newRow = new RawStoreResultSet(row, container.getRowFormatter(row))
              cachedBatchCreator.createInternalRow(newRow)
            }
            else {
              val row: Array[Array[Byte]] = newVal.asInstanceOf[Array[Array[Byte]]]
              val newRow = new RawStoreResultSet(row, container.getRowFormatter(row))
              cachedBatchCreator.createInternalRow(newRow)
            }
          } else {
            InternalRow.empty
          }
        }

        override def hasNext: Boolean = itr.hasNext
      }

      cachedBatchCreator.createCachedBatch(internalRowIterator, batchID, bucketID)
    }
  }
}

/**
 * Created by soubhikc on 19/10/15.
 */
trait StoreCallback {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}
