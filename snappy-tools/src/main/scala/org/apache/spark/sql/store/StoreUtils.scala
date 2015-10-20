package org.apache.spark.sql.store.util

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.scheduler.cluster.SnappyCoarseGrainedSchedulerBackend
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, RangePartitioning, HashPartitioning}
import org.apache.spark.sql.store.{StoreInitRDD, MembershipAccumulator}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{AccumulatorParam, SparkContext, Partition}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.collection.ExecutorLocalPartition

/**
 * Created by rishim on 6/10/15.
 */
object StoreUtils {

  def lookupName(tableName :String, schema : String): String ={
    val lookupName = {
      if (tableName.indexOf(".") <= 0) {
        schema + "." + tableName
      } else tableName
    }.toUpperCase
    lookupName
  }

  def getPartitionsPartitionedTable(sc: SparkContext,
      tableName: String, schema: String,
      blockMap: Map[InternalDistributedMember, BlockManagerId]): Array[Partition] = {

    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val numPartitions = region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    for (p <- 0 until numPartitions) {
      val distMember = region.getBucketPrimary(p)
      val prefNode = blockMap.get(distMember)

      partitions(p) = new ExecutorLocalPartition(p, prefNode.get)
    }
    partitions
  }

  def getPartitionsReplicatedTable(sc : SparkContext , tableName : String, schema : String,  blockMap: Map[InternalDistributedMember, BlockManagerId]) : Array[Partition]= {
    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[DistributedRegion]
    val numPartitions = 1
    val partitions = new Array[Partition](numPartitions)

    val localBackend = sc.schedulerBackend match {
      case lb: LocalBackend => true
      case _ => false
    }

    val member = if(localBackend){
      Misc.getGemFireCache.getDistributedSystem.getDistributedMember
    }else{
      Misc.getGemFireCache.getMembers(region).iterator().next()
    }

    for (p <- 0 until numPartitions) {
      val distMember = member.asInstanceOf[InternalDistributedMember]
      val prefNode = blockMap.get(distMember)
      partitions(p) = new ExecutorLocalPartition(p, prefNode.get)
    }
    partitions
  }

  def initStore(sc: SparkContext, url: String, connProps: Properties): Map[InternalDistributedMember, BlockManagerId] = {
    //TODO for SnappyCluster manager optimize this . Rather than calling this everytime we can get a map from SnappyCluster
    val map = Map[InternalDistributedMember, BlockManagerId]()
    val memberAccumulator = sc.accumulator(map)(MembershipAccumulator)
    new StoreInitRDD(sc, url, connProps)(memberAccumulator).collect()
    memberAccumulator.value

  }
}
