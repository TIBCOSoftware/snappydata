package org.apache.spark.sql.store.util

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, RangePartitioning, HashPartitioning}
import org.apache.spark.{SparkContext, Partition}
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

  def getPartitionsPartitionedTable(sc : SparkContext , tableName : String, schema : String) : Array[Partition]= {
    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
    val numPartitions = region.getTotalNumberOfBuckets
    val partitions = new Array[Partition](numPartitions)

    val numberedPeers = org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(sc).
        keySet.zipWithIndex
    val hostSet = numberedPeers.map(m => {
      Tuple2(m._1.host , m._1)
    }).toMap

    val localBackend = sc.schedulerBackend match {
      case lb: LocalBackend => true
      case _ => false
    }

    for (p <- 0 until numPartitions) {
      //TODO there should be a cleaner way to translate GemFire membership IDs to BlockManagerIds
      //TODO apart from primary members secondary nodes should also be included in preferred node list
      val distMember = region.getBucketPrimary(p)
      val prefNode = if(localBackend){
        Option(hostSet.head._2)
      }else{
        hostSet.get(distMember.getIpAddress.getHostAddress)
      }
      partitions(p) = new ExecutorLocalPartition(p, prefNode.get)
    }
    partitions
  }

  def getPartitionsReplicatedTable(sc : SparkContext , tableName : String, schema : String) : Array[Partition]= {
    val resolvedName = lookupName(tableName, schema)
    val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[DistributedRegion]
    val numPartitions = 1
    val partitions = new Array[Partition](numPartitions)



    val numberedPeers = org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(sc).
        keySet.zipWithIndex
    val hostSet = numberedPeers.map(m => {
      Tuple2(m._1.host, m._1)
    }).toMap

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
      //TODO there should be a cleaner way to translate GemFire membership IDs to BlockManagerIds
      //TODO apart from primary members secondary nodes should also be included in preferred node list
      val distMember = member.asInstanceOf[InternalDistributedMember]
      val prefNode = if (localBackend) {
        Option(hostSet.head._2)
      } else {
        hostSet.get(distMember.getIpAddress.getHostAddress)
      }
      partitions(p) = new ExecutorLocalPartition(p, prefNode.get)
    }
    partitions
  }
}
