/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.store

import java.sql.Connection

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.{DataFrame, Row}
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
 * @param df the dataframe which we want to save to snappy store
 * @param tableName the table name to which we want to write
 * @param getConnection function to get connection
 * @param schema schema of the table
 * @param preservePartitioning If user has specified to preserve the partitioning of incoming RDD

 */

class StoreRDD(@transient sc: SparkContext,
    @transient df: DataFrame,
    tableName: String,
    getConnection: () => Connection,
    schema: StructType,
    preservePartitioning: Boolean,
    blockMap: Map[InternalDistributedMember, BlockManagerId],
    partitionColumns: Seq[String]
    ) extends RDD[Row](sc, Nil) {

  val inputRDD = df.rdd

  println("incoming rdd partition " + inputRDD.partitions.length)

  private val totalNumPartitions =
    executeWithConnection(getConnection, { conn =>
      val tableSchema = conn.getSchema
      val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
      val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
      region.getTotalNumberOfBuckets
    })


  private val part: Partitioner = new ColumnPartitioner(totalNumPartitions)

  private var serializer: Option[Serializer] = None // TDOD use tungsten or Spark inbuilt .
  // Will be easy once this transform into a SparkPlan

  override val partitioner = Some(part)


  override def getDependencies: Seq[Dependency[_]] = {
    if (preservePartitioning && inputRDD.partitions.length != totalNumPartitions) {
      throw new RuntimeException(s"Preserve partitions" +
          s" can be set if partition of input " +
          s"dataset is equal to partitions of table ")
    }
    if (inputRDD.partitioner == Some(part) || preservePartitioning || partitionColumns.isEmpty) {
      logDebug("Adding one-to-one dependency with " + inputRDD)
      List(new OneToOneDependency(inputRDD))
    } else {
      logDebug("Adding shuffle dependency with " + inputRDD)
      //TDOD make this code as part of a SparkPlan so that we can map this to batch inserts and also can use Spark's code-gen feature.
      //See Exchange.scala for more details
      val rddWithPartitionIds: RDD[Product2[Int, Row]] = {

        inputRDD.mapPartitions { iter =>
          val mutablePair = new MutablePair[Int, Row]()

          val ordinals = partitionColumns.map(col => {
            schema.getFieldIndex(col).getOrElse {
              throw new RuntimeException(s"Partition column $col not found in schema $schema")
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
    executeWithConnection(getConnection, { conn =>
      val tableSchema = conn.getSchema
      val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
      val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
      val partitions = new Array[Partition](totalNumPartitions)

      for (p <- 0 until inputRDD.partitions.length) {
        val distMember = region.getBucketPrimary(p)
        partitions(p) = getPartition(p, distMember)
      }
      partitions
    })
  }

  private def getPartition(index: Int, distMember: InternalDistributedMember): Partition = {

    val prefNode = blockMap.get(distMember)
    val narrowDep = dependencies.head match {
      case s: ShuffleDependency[_, _, _] =>
        None
      case _ =>
        Some(new NarrowExecutorLocalSplitDep(inputRDD, index, inputRDD.partitions(index)))
    }
    new CoGroupExecutorLocalPartition(index, prefNode.get, narrowDep)
  }
}





