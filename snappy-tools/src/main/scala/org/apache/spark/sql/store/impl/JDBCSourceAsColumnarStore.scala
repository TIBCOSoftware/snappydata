package org.apache.spark.sql.store.impl

import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import java.util.concurrent.locks.ReentrantLock

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{AbstractRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{MultiExecutorLocalPartition, ExecutorLocalPartition, UUIDRegionKey}
import org.apache.spark.sql.columnar.ConnectionType.ConnectionType
import org.apache.spark.sql.columnar.{CachedBatch, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.store.{CachedBatchIteratorOnRS, JDBCSourceAsStore, ExternalStore}
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Columnar Store implementation for GemFireXD.
 *
 */
final class JDBCSourceAsColumnarStore(_url: String,
    _driver: String,
    _poolProps: Map[String, String],
    _connProps: Properties,
    _hikariCP: Boolean,
    val blockMap: Map[InternalDistributedMember, BlockManagerId]) extends JDBCSourceAsStore(_url, _driver, _poolProps, _connProps, _hikariCP) {

  override def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      uuidList: ArrayBuffer[RDD[UUIDRegionKey]],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD[CachedBatch](sparkContext,
          tableName, requiredColumns, this)
      case _ =>
        var rddList = new ArrayBuffer[RDD[CachedBatch]]()
        uuidList.foreach(x => {
          val y = x.mapPartitions { uuidItr =>
            getCachedBatchIterator(tableName, requiredColumns, uuidItr)
          }
          rddList += y
        })
        new UnionRDD[CachedBatch](sparkContext, rddList)
    }
  }

  override def storeCachedBatch(batch: CachedBatch,
      tableName: String): UUIDRegionKey = {
    val connection: java.sql.Connection = getConnection(tableName)
    try {
      val uuid = connectionType match {

        case ConnectionType.Embedded =>
          val resolvedName = StoreUtils.lookupName(tableName, connection.getSchema)
          val region = Misc.getRegionForTable(resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case pr: PartitionedRegion =>
              val primaryBuckets = pr.getDataStore.getAllLocalPrimaryBucketIds
                  .toArray(new Array[Integer](0))
              genUUIDRegionKey(rand.nextInt(primaryBuckets.size))
            case _ =>
              genUUIDRegionKey()
          }

        case _ => genUUIDRegionKey()
      }

      val rowInsertStr = getRowInsertStr(tableName, batch.buffers.length)
      val stmt = connection.prepareStatement(rowInsertStr)
      stmt.setString(1, uuid.getUUID.toString)
      stmt.setInt(2, uuid.getBucketId)
      stmt.setBytes(3, serializer.newInstance().serialize(batch.stats).array())
      var columnIndex = 4
      batch.buffers.foreach(buffer => {
        stmt.setBytes(columnIndex, buffer)
        columnIndex += 1
      })
      stmt.executeUpdate()
      stmt.close()
      uuid
    } finally {
      connection.close()
    }
  }


}


class ColumnarStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
    tableName: String,
    requiredColumns: Array[String], store: JDBCSourceAsColumnarStore)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
        val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
        val par = split.index
        val ps1 = conn.prepareStatement(s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', $par)")
        ps1.execute()
        val ps = conn.prepareStatement(s"select stats , " +
            requiredColumns.mkString(" ", ",", " ") +
            s" from $tableName")

        val rs = ps.executeQuery()

        new CachedBatchIteratorOnRS(conn, store.connectionType, requiredColumns, ps, rs)
    }, closeOnSuccess = false)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiExecutorLocalPartition].hostExecutorIds
  }

  override protected def getPartitions: Array[Partition] = {
    store.tryExecute(tableName, {
      case conn =>
        val tableSchema = conn.getSchema
        StoreUtils.getPartitionsPartitionedTable(_sc, tableName, tableSchema, store.blockMap)
    })

  }
}
