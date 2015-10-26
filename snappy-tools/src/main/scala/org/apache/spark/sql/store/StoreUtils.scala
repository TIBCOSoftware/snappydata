package org.apache.spark.sql.store.util

import java.util
import java.util.Properties

import scala.collection.mutable

import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import scala.collection.JavaConversions._
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.store.{StoreInitRDD, MembershipAccumulator}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{SparkContext, Partition}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.collection.{MultiExecutorLocalPartition}

/**
 * Created by rishim on 6/10/15.
 */
object StoreUtils {

  val PARTITION_BY = "PARTITION_BY"
  val BUCKETS = "BUCKETS"
  val COLOCATE_WITH = "COLOCATE_WITH"
  val REDUNDANCY = "REDUNDANCY"
  val RECOVERYDELAY = "RECOVERYDELAY"
  val MAXPARTSIZE = "MAXPARTSIZE"
  val EVICTION_BY = "EVICTION_BY"
  val PERSISTENT = "PERSISTENT"
  val SERVER_GROUPS = "SERVER_GROUPS"
  val OFFHEAP = "OFFHEAP"

  val GEM_PARTITION_BY = "PARTITION BY"
  val GEM_BUCKETS = "BUCKETS"
  val GEM_COLOCATE_WITH = "COLOCATE WITH"
  val GEM_REDUNDANCY = "REDUNDANCY"
  val GEM_RECOVERYDELAY = "RECOVERYDELAY"
  val GEM_MAXPARTSIZE = "MAXPARTSIZE"
  val GEM_EVICTION_BY = "EVICTION BY"
  val GEM_PERSISTENT = "PERSISTENT"
  val GEM_SERVER_GROUPS = "SERVER GROUPS"
  val GEM_OFFHEAP = "OFFHEAP"
  val PRIMARY_KEY = "PRIMARY KEY"

  val EMPTY_STRING = ""


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

  def appendClause(sb : mutable.StringBuilder, getClause : () => String) : Unit = {
    val clause = getClause.apply()
    if(!clause.isEmpty){
      sb.append(s"$clause " )
    }
  }

  def ddlExtensionString(parameters: Map[String, String]): String = {
    val sb = new StringBuilder()

    val options = new CaseInsensitiveMap(parameters)

    options.keySet.foreach { prop =>
      prop.toUpperCase match {
        case PARTITION_BY | BUCKETS | COLOCATE_WITH | REDUNDANCY | RECOVERYDELAY | MAXPARTSIZE | EVICTION_BY | PERSISTENT | SERVER_GROUPS | OFFHEAP => // Do nothing. Allowed values
        case _ => throw new IllegalArgumentException(
          s"Illegal property $prop while creating table")
      }
    }

    appendClause(sb, () => {
      val partitionby = options.getOrElse(PARTITION_BY, EMPTY_STRING)
      if (partitionby.isEmpty){
        EMPTY_STRING
      }else{
        val parclause =
          if (partitionby.equals(PRIMARY_KEY)) {
            PRIMARY_KEY
          } else {
            s"COLUMN ($partitionby)"
          }
        s"$GEM_PARTITION_BY $parclause"
      }

    })

    appendClause(sb, () => {
      if (options.get(BUCKETS).isDefined) {
        s"$GEM_BUCKETS ${options.get(BUCKETS).get}"
      } else {
        EMPTY_STRING
      }
    })


    appendClause(sb, () => {
      if (options.get(COLOCATE_WITH).isDefined) {
        s"$GEM_COLOCATE_WITH ${options.get(COLOCATE_WITH).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(REDUNDANCY).isDefined) {
        s"$GEM_REDUNDANCY ${options.get(REDUNDANCY).get}"
      } else {
        EMPTY_STRING
      }
    })


    appendClause(sb, () => {
      if (options.get(RECOVERYDELAY).isDefined) {
        s"$GEM_RECOVERYDELAY ${options.get(RECOVERYDELAY).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(MAXPARTSIZE).isDefined) {
        s"$GEM_MAXPARTSIZE ${options.get(MAXPARTSIZE).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(EVICTION_BY).isDefined) {
        s"$GEM_EVICTION_BY ${options.get(EVICTION_BY).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(PERSISTENT).isDefined) {
        s"$GEM_PERSISTENT ${options.get(PERSISTENT).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(SERVER_GROUPS).isDefined) {
        s"$GEM_SERVER_GROUPS ${options.get(SERVER_GROUPS).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(OFFHEAP).isDefined) {
        s"$GEM_OFFHEAP ${options.get(OFFHEAP).get}"
      } else {
        EMPTY_STRING
      }
    })

    sb.toString()
  }
}
