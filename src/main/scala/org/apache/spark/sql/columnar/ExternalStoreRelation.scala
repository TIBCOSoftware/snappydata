package org.apache.spark.sql.columnar

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Accumulable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.InMemoryAppendableRelation.CachedBatchHolder
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

private[sql] final class ExternalStoreRelation(
    override val output: Seq[Attribute],
    override val useCompression: Boolean,
    override val batchSize: Int,
    override val storageLevel: StorageLevel,
    override val child: SparkPlan,
    override val tableName: Option[String],
    val isSampledTable: Boolean,
    val externalStore: ExternalStore)(
    private var _ccb: RDD[CachedBatch] = null,
    private var _stats: Statistics = null,
    private var _bstats: Accumulable[ArrayBuffer[InternalRow], InternalRow] = null,
    private var uuidList: ArrayBuffer[RDD[UUIDRegionKey]]
     = new ArrayBuffer[RDD[UUIDRegionKey]]())
    extends InMemoryAppendableRelation(
     output, useCompression, batchSize, storageLevel, child, tableName,
     isSampledTable)(_ccb: RDD[CachedBatch],
        _stats: Statistics,
        _bstats: Accumulable[ArrayBuffer[InternalRow], InternalRow]) {

  override def appendBatch(batch: RDD[CachedBatch]) = writeLock {
    throw new IllegalStateException(
      s"did not expect appendBatch of ExternalStoreRelation to be called")
  }

  def appendUUIDBatch(batch: RDD[UUIDRegionKey]) = writeLock {
    uuidList += batch
  }

  override def truncate() = writeLock {
    for (batch <- uuidList) {
      // TODO: Go to GemXD and remove
    }
    uuidList.clear()
  }

  override def recache(): Unit = {
    sys.error(
      s"ExternalStoreRelation: unexpected call to recache for $tableName")
  }

  override def withOutput(newOutput: Seq[Attribute]) = {
    new ExternalStoreRelation(newOutput, useCompression, batchSize,
      storageLevel, child, tableName, isSampledTable, externalStore)(
          cachedColumnBuffers, super.statisticsToBePropagated, batchStats, uuidList)
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
      externalStore)(cachedColumnBuffers, super.statisticsToBePropagated,
          batchStats, uuidList).asInstanceOf[this.type]
  }

  override def cachedColumnBuffers: RDD[CachedBatch] = readLock {
    externalStore.getCachedBatchRDD(tableName.get, uuidList, this.child.sqlContext.sparkContext)
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

  def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
      batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
    val uuid = externalStore.storeCachedBatch(batch,
      tableName.getOrElse(throw new IllegalStateException("missing tableName")))
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
      jdbcSource: ExternalStore): ExternalStoreRelation =
    new ExternalStoreRelation(child.output, useCompression, batchSize,
      storageLevel, child, tableName, isSampledTable, jdbcSource)()

  def apply(useCompression: Boolean,
      batchSize: Int,
      tableName: QualifiedTableName,
      schema: StructType,
      relation: InMemoryRelation,
      output: Seq[Attribute]): CachedBatchHolder[ArrayBuffer[Serializable]] = {
    def columnBuilders = output.map { attribute =>
      val columnType = ColumnType(attribute.dataType)
      val initialBufferSize = columnType.defaultSize * batchSize
      ColumnBuilder(attribute.dataType, initialBufferSize,
        attribute.name, useCompression)
    }.toArray

    val holder = relation match {
      case esr: ExternalStoreRelation =>
        new CachedBatchHolder(columnBuilders, 0, batchSize, schema,
          new ArrayBuffer[UUIDRegionKey](1), esr.uuidBatchAggregate)
      case imar: InMemoryAppendableRelation =>
        new CachedBatchHolder(columnBuilders, 0, batchSize, schema,
          new ArrayBuffer[CachedBatch](1), imar.batchAggregate)
      case _ => throw new IllegalStateException("ExternalStoreRelation:" +
          s" unknown relation $relation for table $tableName")
    }
    holder.asInstanceOf[CachedBatchHolder[ArrayBuffer[Serializable]]]
  }
}

private[sql] class ExternalStoreTableScan(
    override val attributes: Seq[Attribute],
    override val predicates: Seq[Expression],
    override val relation: InMemoryAppendableRelation)
    extends InMemoryAppendableColumnarTableScan(attributes, predicates,
      relation) {
}

private[sql] object ExternalStoreTableScan {
  def apply(attributes: Seq[Attribute], predicates: Seq[Expression],
      relation: InMemoryAppendableRelation): SparkPlan = {
    new ExternalStoreTableScan(attributes, predicates, relation)
  }
}
