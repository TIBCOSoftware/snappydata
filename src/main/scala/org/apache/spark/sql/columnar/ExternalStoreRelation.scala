package org.apache.spark.sql.columnar

import java.nio.ByteBuffer
import java.util.{Properties, UUID}
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.sql.Row
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.store.impl.GemXDSource_LC
import org.apache.spark.{TaskContext, Accumulable}
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[sql] class ExternalStoreRelation(
    override val output: Seq[Attribute],
    override val useCompression: Boolean,
    override val batchSize: Int,
    override val storageLevel: StorageLevel,
    override val child: SparkPlan,
    override val tableName: Option[String],
    val isSampledTable: Boolean,
    private val connURL: String,
    private val connProps: Properties)(
    private var _ccb: RDD[CachedBatch] = null,
    private var _stats: Statistics = null,
    private var _bstats: Accumulable[ArrayBuffer[Row], Row] = null,
    private var _cachedBufferList: ArrayBuffer[RDD[CachedBatch]] = null)
    extends InMemoryAppendableRelation(
     output, useCompression, batchSize, storageLevel, child, tableName,
     isSampledTable)(_ccb: RDD[CachedBatch],
        _stats: Statistics,
        _bstats: Accumulable[ArrayBuffer[Row], Row]) {

  private var _uuidList: ArrayBuffer[RDD[UUID]] =
    new ArrayBuffer[RDD[UUID]]()

  private lazy val externalStore: ExternalStore = getExternalStore(connURL, connProps)

  private def getExternalStore(connURL: String, connProps: Properties): ExternalStore = {
    // For now construct GemXD_LC source as the method can resolve from the url
    new GemXDSource_LC(connURL, connProps, Map[String, String](), false)
  }

  override def appendBatch(batch: RDD[CachedBatch]) = writeLock {
    throw new IllegalStateException(s"did not expect appendBatch of ExternalStoreRelation to be called")
  }

  override def appendUUIDBatch(batch: RDD[UUID]) = writeLock {
    _uuidList += batch
  }

  override def truncate() = writeLock {
    for (batch <- _uuidList) {
      // TODO: Go to GemXD and remove
    }
    _uuidList.clear()
  }

  override def recache(): Unit = {
    sys.error(
      s"ExternalStoreRelation: unexpected call to recache for $tableName")
  }

  override def withOutput(newOutput: Seq[Attribute]): InMemoryAppendableRelation = {
    new ExternalStoreRelation(newOutput, useCompression, batchSize,
      storageLevel, child, tableName, isSampledTable, connURL, connProps)(cachedColumnBuffers,
      super.statisticsToBePropagated, batchStats, null)
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override def newInstance(): this.type = {
    new ExternalStoreRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName,
      isSampledTable,
      connURL,
      connProps)(cachedColumnBuffers,
        super.statisticsToBePropagated, batchStats, null).asInstanceOf[this.type]
  }

  private def getCachedBatchIteratorFromuuidItr(itr: Iterator[UUID]): Iterator[CachedBatch] = {
    externalStore.getCachedBatchIterator(tableName.get, itr, false)
  }

  // TODO: Check if this is allright
  override def cachedColumnBuffers: RDD[CachedBatch] = readLock {
    var rddList = new ArrayBuffer[RDD[CachedBatch]]()
      _uuidList.foreach(x => {
        val y = x.mapPartitions { uuidItr =>
          getCachedBatchIteratorFromuuidItr(uuidItr)
        }
        rddList += y
      })
    new UnionRDD[CachedBatch](this.child.sqlContext.sparkContext, rddList)
  }

  // TODO: Do this later...understand whats the need of this function
  //  override protected def otherCopyArgs: Seq[AnyRef] =
  //    Seq(super.cachedColumnBuffers, super.statisticsToBePropagated,
  //      batchStats, _cachedBufferList)

  override private[sql] def uncache(blocking: Boolean): Unit = {
    super.uncache(blocking)
    writeLock {
      // TODO: Go to GemXD and truncate or drop
    }
  }

  def getUUIDList = {
    _uuidList
  }

  def uuidBatchAggregate(accumulated: ArrayBuffer[UUID], batch: CachedBatch): ArrayBuffer[UUID] = {
    val uuid = externalStore.storeCachedBatch(batch, tableName.getOrElse(throw new IllegalStateException("tableName required")))
    accumulated += uuid
  }
}

private[sql] object ExternalStoreRelation {
  def apply(useCompression: Boolean,
            batchSize: Int,
            storageLevel: StorageLevel,
            child: SparkPlan,
            tableName: Option[String],
            isSampledTable: Boolean,
            connURL: String, connProps: Properties): ExternalStoreRelation =
    new ExternalStoreRelation(child.output, useCompression, batchSize,
      storageLevel, child, tableName, isSampledTable, connURL, connProps)()
}

private[sql] class ExternalStoreTableScan(
                                           override val attributes: Seq[Attribute],
                                           override val predicates: Seq[Expression],
                                           override val relation: InMemoryAppendableRelation)
  extends InMemoryAppendableColumnarTableScan(attributes, predicates, relation) {

}


private[sql] object ExternalStoreTableScan {
  def apply(attributes: Seq[Attribute], predicates: Seq[Expression],
            relation: InMemoryAppendableRelation): SparkPlan = {
    new InMemoryAppendableColumnarTableScan(attributes, predicates, relation)
  }
}
