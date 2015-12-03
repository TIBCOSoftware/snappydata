package org.apache.spark.sql.store

import java.sql.Connection

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.collection.{CoGroupExecutorLocalPartition, NarrowExecutorLocalSplitDep}
import org.apache.spark.sql.store.StoreFunctions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.MutablePair

/**
 * Generic RDD for doing bulk inserts in Snappy-Store. This RDD creates dependencies according to input RDD's partitioner.
 *
 *
 * @param sc Snappy context
 * @param prev the RDD which we want to save to snappy store
 * @param tableName the table name to which we want to write
 * @param getConnection function to get connection
 * @param schema schema of the table
 * @param preservePartitioning If user has specified to preserve the partitioning of incoming RDD

 */

class StoreRDD(@transient sc: SparkContext,
    @transient prev: RDD[Row],
    tableName: String,
    getConnection: () => Connection,
    schema: StructType,
    preservePartitioning: Boolean,
    blockMap: Map[InternalDistributedMember, BlockManagerId]) extends RDD[Row](sc, Nil) {

  val (partitionColumn, totalNumBucket) = getPartitioningInfo(getConnection, tableName)

  private val part: Partitioner = new ColumnPartitioner(totalNumBucket)

  private var serializer: Option[Serializer] = None // TDOD use tungsten or Spark inbuilt . Will be easy once this transform into a SparkPlan

  override val partitioner = Some(part)

  def getPartitioningInfo(getConnection: () => Connection, tableName: String): (Seq[String], Int) = {
    executeWithConnection(getConnection, {
      case conn => val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true)
        val (partitionColumn, totalNumBucket) = if (region.isInstanceOf[PartitionedRegion]) {
          val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
          val numPartitions = region.getTotalNumberOfBuckets
          val resolver = region.getPartitionResolver.asInstanceOf[GfxdPartitionByExpressionResolver]
          val parColumn = resolver.getColumnNames

          (parColumn.toSeq, numPartitions)

        } else {
          (Seq.empty[String], 0)
        }
        (partitionColumn, totalNumBucket)
    })
  }

  override def getDependencies: Seq[Dependency[_]] = {
    if (preservePartitioning && prev.partitions.length != totalNumBucket) {
      throw new RuntimeException(s"Preserve partitions can be set if partition of input dataset is equal to partitions of table ")
    }
    if (prev.partitioner == Some(part) || preservePartitioning || partitionColumn.isEmpty) {
      logDebug("Adding one-to-one dependency with " + prev)
      List(new OneToOneDependency(prev))
    } else {
      logDebug("Adding shuffle dependency with " + prev)
      //TDOD make this code as part of a SparkPlan so that we can map this to batch inserts and also can use Spark's code-gen feature.
      //See Exchange.scala for more details
      val rddWithPartitionIds: RDD[Product2[Int, Row]] = {

        prev.mapPartitions { iter =>
          val mutablePair = new MutablePair[Int, Row]()

          val ordinals = partitionColumn.map(col => {
            schema.getFieldIndex(col.toUpperCase).getOrElse {
              throw new RuntimeException(s"Partition column $col} not found in schema $schema")
            }

          })
          iter.map { row =>
            val parKey = ordinals.map(k => row.get(k))
            mutablePair.update(part.getPartition(parKey), row)
          }
        }
      }

      List(new ShuffleDependency[Int, Row, Row](rddWithPartitionIds, part, serializer))
    }
  }

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    val dep = dependencies.head
    dep match {
      case oneToOneDependency: OneToOneDependency[_] =>
        val dependencyPartition = split.asInstanceOf[CoGroupExecutorLocalPartition].narrowDep.get.split
        oneToOneDependency.rdd.iterator(dependencyPartition, context).asInstanceOf[Iterator[Row]]

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        SparkEnv.get.shuffleManager.getReader(shuffleDependency.shuffleHandle, split.index, split.index + 1, context)
            .read()
            .asInstanceOf[Iterator[Product2[Int, Row]]]
            .map(_._2)

    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[CoGroupExecutorLocalPartition].hostExecutorId)
  }


  lazy val localBackend = sc.schedulerBackend match {
    case lb: LocalBackend => true
    case _ => false
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = {
    executeWithConnection(getConnection, {
      case conn =>
        val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true)

        if (region.isInstanceOf[PartitionedRegion]) {
          val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
          val numPartitions = region.getTotalNumberOfBuckets

          val partitions = new Array[Partition](numPartitions)

          for (p <- 0 until numPartitions) {
            val distMember = region.getBucketPrimary(p)
            partitions(p) = getPartition(p, distMember)
          }
          partitions
        } else {
          val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[DistributedRegion]
          val partitions = new Array[Partition](prev.partitions.length)

          val member = if (localBackend) {
            Misc.getGemFireCache.getDistributedSystem.getDistributedMember
          } else {
            Misc.getGemFireCache.getMembers(region).iterator().next()
          }


          for (p <- 0 until partitions.length) {
            val distMember = member.asInstanceOf[InternalDistributedMember]
            partitions(p) = getPartition(p, distMember)
          }
          partitions
        }
    })

  }

  private def getPartition(index: Int, distMember: InternalDistributedMember): Partition = {

    val prefNode = blockMap.get(distMember)

    val narrowDep = dependencies.head match {
      case s: ShuffleDependency[_, _, _] =>
        None
      case _ =>
        Some(new NarrowExecutorLocalSplitDep(prev, index, prev.partitions(index)))
    }
    new CoGroupExecutorLocalPartition(index, prefNode.get, narrowDep)
  }
}

/**
 * :: DeveloperApi ::
 * Used by test code to determine dependency..
 */
object StoreRDD {

  def apply(sc: SparkContext, prev: RDD[Row], schema: StructType, partitionColumn: Option[String], preservePartitioning: Boolean, numPartitions: Int): StoreRDD = {
    new StoreRDD(
      sc,
      prev,
      "",
      null,
      schema,
      preservePartitioning,
      null
    )
  }
}



