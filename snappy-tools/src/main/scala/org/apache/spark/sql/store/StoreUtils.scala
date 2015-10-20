package org.apache.spark.sql.store.util

import java.util
import java.util.Properties

import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import scala.collection.JavaConversions._
import org.apache.spark.sql.store.{StoreInitRDD, MembershipAccumulator}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{SparkContext, Partition}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.collection.{MultiExecutorLocalPartition}

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
      val distMembers = region.getRegionAdvisor.getBucketOwners(p)

      val prefNodes = distMembers.map(
        distMember => blockMap.get(distMember)
      )

      val prefNodeSeq = prefNodes.map(a => a.get).toSeq
      partitions(p) = new MultiExecutorLocalPartition(p, prefNodeSeq)
    }
    partitions
  }

  def getPartitionsReplicatedTable(sc : SparkContext ,
      tableName : String, schema : String,
      blockMap: Map[InternalDistributedMember, BlockManagerId]) : Array[Partition]= {

    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[DistributedRegion]
    val numPartitions = 1
    val partitions = new Array[Partition](numPartitions)

    val localBackend = sc.schedulerBackend match {
      case lb: LocalBackend => true
      case _ => false
    }

    val distMembers = if(localBackend){
      val set = new util.HashSet[DistributedMember]()
      set.add(Misc.getGemFireCache.getDistributedSystem.getDistributedMember)
      set
    }else{
      Misc.getGemFireCache.getMembers(region)
    }

    for (p <- 0 until numPartitions) {

      val prefNodes = distMembers.map(
        distMember => blockMap.get(distMember)
      ).toSeq

      partitions(p) = new MultiExecutorLocalPartition(p, prefNodes)
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
