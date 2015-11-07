package org.apache.spark.sql.store.util

import java.sql.Connection
import java.util
import java.util.Properties

import scala.collection.mutable
import scala.reflect.ClassTag

import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import scala.collection.JavaConversions._
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}
import org.apache.spark.sql.sources.{JdbcExtendedUtils, JdbcExtendedDialect}
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
      val withBlockManagers = distMembers.filter(d => blockMap.contains(d))

      val prefNodes = distMembers.map(
        m => blockMap.get(m)
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

    val withBlockManagers = distMembers.filter(d => blockMap.contains(d))

    for (p <- 0 until numPartitions) {

      val prefNodes = distMembers.map(
        m => blockMap.get(m)
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

  def removeInternalProps(parameters : mutable.HashMap[String, String])  = {
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")
    table
  }


  def ddlExtensionString(parameters: mutable.HashMap[String, String]): String = {
    val sb = new StringBuilder()

    sb.append(parameters.remove(PARTITION_BY).map(v => {
      val parClause = {
        v match {
          case PRIMARY_KEY => PRIMARY_KEY
          case _ => s"COLUMN ($v)"
        }
      }
      s"$GEM_PARTITION_BY $parClause "
    }
    ).getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(BUCKETS).map(v => s"$GEM_BUCKETS $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(COLOCATE_WITH).map(v => s"$GEM_COLOCATE_WITH $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(REDUNDANCY).map(v => s"$GEM_REDUNDANCY $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(RECOVERYDELAY).map(v => s"$GEM_RECOVERYDELAY $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(MAXPARTSIZE).map(v => s"$GEM_MAXPARTSIZE $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(EVICTION_BY).map(v => s"$GEM_EVICTION_BY $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(PERSISTENT).map(v => s"$GEM_PERSISTENT $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(SERVER_GROUPS).map(v => s"$GEM_SERVER_GROUPS $v ").getOrElse(EMPTY_STRING))
    sb.append(parameters.remove(OFFHEAP).map(v => s"$GEM_OFFHEAP $v ").getOrElse(EMPTY_STRING))

    sb.toString()
  }
}
