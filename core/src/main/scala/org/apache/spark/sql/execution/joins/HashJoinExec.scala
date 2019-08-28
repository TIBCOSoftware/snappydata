/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.execution.joins

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutionException}

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.google.common.cache.CacheBuilder
import io.snappydata.collection.ObjectHashSet

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, BindReferences, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.streaming.PhysicalDStreamPlan
import org.apache.spark.sql.types.TypeUtilities
import org.apache.spark.sql.{DelegateRDD, SnappySession}

/**
 * :: DeveloperApi ::
 * Performs a local hash join of two child relations. If a relation
 * (out of a datasource) is already replicated across all nodes then rather
 * than doing a Broadcast join which can be expensive, this join just
 * scans through the single partition of the replicated relation while
 * streaming through the other relation.
 */
@DeveloperApi
case class HashJoinExec(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan,
    leftSizeInBytes: BigInt,
    rightSizeInBytes: BigInt,
    replicatedTableJoin: Boolean)
    extends NonRecursivePlans with BinaryExecNode with HashJoin
        with SnappyJoinLike with BatchConsumer {

  override def nodeName: String = "SnappyHashJoin"

  @transient private var mapAccessor: ObjectHashMapAccessor = _
  @transient private var hashMapTerm: String = _
  @transient private var mapDataTerm: String = _
  @transient private var maskTerm: String = _
  @transient private var initMap: String = _
  @transient private var initMapCode: String = _
  @transient private var mapSize: String = _
  @transient private var keyIsUniqueTerm: String = _
  @transient private var numRowsTerm: String = _
  @transient private var dictionaryArrayTerm: String = _
  @transient private var dictionaryArrayInit: String = _

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def outputPartitioning: Partitioning = {
    if (replicatedTableJoin) {
      streamedPlan.outputPartitioning
    } else joinType match {
      case Inner => PartitioningCollection(
        Seq(left.outputPartitioning, right.outputPartitioning))
      // For left and right outer joins, the output is partitioned
      // by the streamed input's join keys.
      case LeftOuter | RightOuter => streamedPlan.outputPartitioning
      case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
      case LeftExistence(_) => left.outputPartitioning
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  // hash join does not change ordering of the streamed plan
  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil
    case _ => streamedPlan.outputOrdering
  }

  override def requiredChildDistribution: Seq[Distribution] =
    if (replicatedTableJoin) {
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      // SnappyJoinLike.requiredChildDistribution has the required logic to deal with
      // join keys a subset of existing child partitioning
      super.requiredChildDistribution
    }

  protected lazy val (buildSideKeys, streamSideKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  // return empty here as code of required variables is explicitly instantiated
  override def usedInputs: AttributeSet = AttributeSet.empty


  private def findShuffleDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
    rdd.dependencies.flatMap {
      case s: ShuffleDependency[_, _, _] => if (s.rdd ne rdd) {
        s +: findShuffleDependencies(s.rdd)
      } else s :: Nil
      case d => findShuffleDependencies(d.rdd)
    }
  }

  private lazy val (streamSideRDDs, buildSideRDDs) = {
    val streamRDDs = streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
    val buildRDDs = buildPlan.asInstanceOf[CodegenSupport].inputRDDs()

    if (replicatedTableJoin) {
      val streamRDD = streamRDDs.head
      val numParts = streamRDD.getNumPartitions
      val buildShuffleDeps: Seq[Dependency[_]] = buildRDDs.flatMap(findShuffleDependencies)
      val preferredLocations = Array.tabulate[Seq[String]](numParts) { i =>
        streamRDD.preferredLocations(streamRDD.partitions(i))
      }
      val streamPlanRDDs = if (buildShuffleDeps.nonEmpty) {
        // add the build-side shuffle dependencies to first stream-side RDD
        new DelegateRDD[InternalRow](streamRDD.sparkContext, streamRDD, buildRDDs,
          preferredLocations, streamRDD.dependencies ++ buildShuffleDeps) +:
          streamRDDs.tail.map(rdd => new DelegateRDD[InternalRow](
            rdd.sparkContext, rdd, buildRDDs, preferredLocations))
      } else {
        new DelegateRDD[InternalRow](streamRDD.sparkContext, streamRDD, buildRDDs,
          preferredLocations) +: streamRDDs.tail
      }
      (streamPlanRDDs, buildRDDs)
    } else {
      // wrap in DelegateRDD for shuffle dependencies and preferred locations

      // Get the build side shuffle dependencies.
      val buildShuffleDeps: Seq[Dependency[_]] = buildRDDs.flatMap(findShuffleDependencies)
      val hasStreamSideShuffle = streamRDDs.exists(_.dependencies
        .exists(_.isInstanceOf[ShuffleDependency[_, _, _]]))
      // treat as a zip of all stream side RDDs and build side RDDs and
      // use intersection of preferred locations, if possible, else union

      // Mostly with SHJ both the partition num will be equal.
      // However, in certain cases if num partition of one side is
      // == 1 it also qualifies for SHJ.
      val (allRDDs, numParts) = if (buildRDDs.head.getNumPartitions == 1) {
        (streamRDDs, streamRDDs.head.getNumPartitions)
      } else if (streamRDDs.head.getNumPartitions == 1) {
        (buildRDDs, buildRDDs.head.getNumPartitions)
      } else {
        // Equal partitions
        (streamRDDs ++ buildRDDs, streamRDDs.head.getNumPartitions)
      }
      val preferredLocations = Array.tabulate[Seq[String]](numParts) { i =>
        val prefLocations = allRDDs.map(rdd => rdd.preferredLocations(
          rdd.partitions(i)))
        val exactMatches = prefLocations.reduce(_.intersect(_))
        // prefer non-exchange side locations if no exact matches
        if (exactMatches.nonEmpty) exactMatches
        else if (buildShuffleDeps.nonEmpty) {
          prefLocations.take(streamRDDs.length).flatten.distinct
        } else if (hasStreamSideShuffle) {
          prefLocations.takeRight(buildRDDs.length).flatten.distinct
        } else prefLocations.flatten.distinct
      }
      val streamPlanRDDs = if (buildShuffleDeps.nonEmpty) {
        // add the build-side shuffle dependencies to first stream-side RDD
        val rdd = streamRDDs.head
        new DelegateRDD[InternalRow](rdd.sparkContext, rdd, buildRDDs,
          preferredLocations, rdd.dependencies ++ buildShuffleDeps) +:
          streamRDDs.tail.map(rdd => new DelegateRDD[InternalRow](
            rdd.sparkContext, rdd, buildRDDs, preferredLocations))
      } else {
        streamRDDs.map(rdd => new DelegateRDD[InternalRow](
          rdd.sparkContext, rdd, buildRDDs, preferredLocations))
      }

      (streamPlanRDDs, buildRDDs.map(rdd => new DelegateRDD[InternalRow](
        rdd.sparkContext, rdd, Nil, preferredLocations)))
    }
  }

  private def refreshRDDs() : (Seq[RDD[InternalRow]], Seq[RDD[InternalRow]]) = {
    val streamRDDs = streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
    val buildRDDs = buildPlan.asInstanceOf[CodegenSupport].inputRDDs()

    if (replicatedTableJoin) {
      val streamRDD = streamRDDs.head
      val numParts = streamRDD.getNumPartitions
      val buildShuffleDeps: Seq[Dependency[_]] = buildRDDs.flatMap(findShuffleDependencies)
      val preferredLocations = Array.tabulate[Seq[String]](numParts) { i =>
        streamRDD.preferredLocations(streamRDD.partitions(i))
      }
      val streamPlanRDDs = if (buildShuffleDeps.nonEmpty) {
        // add the build-side shuffle dependencies to first stream-side RDD
        new DelegateRDD[InternalRow](streamRDD.sparkContext, streamRDD, buildRDDs,
          preferredLocations, streamRDD.dependencies ++ buildShuffleDeps) +:
          streamRDDs.tail.map(rdd => new DelegateRDD[InternalRow](
            rdd.sparkContext, rdd, buildRDDs, preferredLocations))
      } else {
        streamRDDs
      }
      (streamPlanRDDs, buildRDDs)
    }
    else {
      // wrap in DelegateRDD for shuffle dependencies and preferred locations

      // Get the build side shuffle dependencies.
      val buildShuffleDeps: Seq[Dependency[_]] = buildRDDs.flatMap(findShuffleDependencies)
      val hasStreamSideShuffle = streamRDDs.exists(_.dependencies
          .exists(_.isInstanceOf[ShuffleDependency[_, _, _]]))
      // treat as a zip of all stream side RDDs and build side RDDs and
      // use intersection of preferred locations, if possible, else union
      val numParts = streamRDDs.head.getNumPartitions
      val allRDDs = streamRDDs ++ buildRDDs
      val preferredLocations = Array.tabulate[Seq[String]](numParts) { i =>
        val prefLocations = allRDDs.map(rdd => rdd.preferredLocations(
          rdd.partitions(i)))
        val exactMatches = prefLocations.reduce(_.intersect(_))
        // prefer non-exchange side locations if no exact matches
        if (exactMatches.nonEmpty) exactMatches
        else if (buildShuffleDeps.nonEmpty) {
          prefLocations.take(streamRDDs.length).flatten.distinct
        } else if (hasStreamSideShuffle) {
          prefLocations.takeRight(buildRDDs.length).flatten.distinct
        } else prefLocations.flatten.distinct
      }
      val streamPlanRDDs = if (buildShuffleDeps.nonEmpty) {
        // add the build-side shuffle dependencies to first stream-side RDD
        val rdd = streamRDDs.head
        new DelegateRDD[InternalRow](rdd.sparkContext, rdd, buildRDDs,
          preferredLocations, rdd.dependencies ++ buildShuffleDeps) +:
            streamRDDs.tail.map(rdd => new DelegateRDD[InternalRow](
              rdd.sparkContext, rdd, buildRDDs, preferredLocations))
      } else {
        streamRDDs.map(rdd => new DelegateRDD[InternalRow](
          rdd.sparkContext, rdd, buildRDDs, preferredLocations))
      }

      (streamPlanRDDs, buildRDDs.map(rdd => new DelegateRDD[InternalRow](
        rdd.sparkContext, rdd, Nil, preferredLocations)))
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    if (streamedPlan.find(_.isInstanceOf[PhysicalDStreamPlan]).isDefined) {
      refreshRDDs()._1
    } else {
      streamSideRDDs
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    initMap = ctx.freshName("initMap")
    ctx.addMutableState("boolean", initMap, s"$initMap = false;")

    val createMap = ctx.freshName("createMap")
    val createMapClass = ctx.freshName("CreateMap")
    val getOrCreateMap = ctx.freshName("getOrCreateMap")

    val beforeMap = ctx.freshName("beforeMap")
    val buildTime = metricTerm(ctx, "buildTime")
    val numOutputRows = metricTerm(ctx, "numOutputRows")

    // generate variable name for hash map for use here and in consume
    hashMapTerm = ctx.freshName("hashMap")
    val hashSetClassName = classOf[ObjectHashSet[_]].getName
    ctx.addMutableState(hashSetClassName, hashMapTerm, "")

    // using the expression IDs is enough to ensure uniqueness
    val buildCodeGen = buildPlan.asInstanceOf[CodegenSupport]
    val rdds = {
      if (buildPlan.find(_.isInstanceOf[PhysicalDStreamPlan]).isDefined) {
        refreshRDDs()._2
      } else {
        buildSideRDDs
      }
    }
    val exprIds = buildPlan.output.map(_.exprId.id).toArray
    val cacheKeyTerm = ctx.addReferenceObj("cacheKey",
      new CacheKey(exprIds, rdds.head.id))

    // generate local variables for HashMap data array and mask
    mapDataTerm = ctx.freshName("mapData")
    maskTerm = ctx.freshName("hashMapMask")
    mapSize = ctx.freshName("mapSize")
    keyIsUniqueTerm = ctx.freshName("keyIsUnique")
    numRowsTerm = ctx.freshName("numRows")

    // generate the map accessor to generate key/value class
    // and get map access methods
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    mapAccessor = ObjectHashMapAccessor(session, ctx, buildSideKeys,
      buildPlan.output, "LocalMap", Nil, hashMapTerm, mapDataTerm, maskTerm,
      multiMap = true, this, this.parent, buildPlan)

    val entryClass = mapAccessor.getClassName
    ctx.addMutableState(s"$entryClass[]", mapDataTerm, "")
    ctx.addMutableState("int", maskTerm, "")
    ctx.addMutableState("int", mapSize, s"$mapSize = -1;")
    ctx.addMutableState("boolean", keyIsUniqueTerm, s"$keyIsUniqueTerm = true;")

    val buildRDDs = ctx.addReferenceObj("buildRDDs", rdds.toArray,
      s"${classOf[RDD[_]].getName}[]")
    val buildParts = rdds.map(_.partitions)
    val partitionClass = classOf[Partition].getName
    val buildPartsVar = ctx.addReferenceObj("buildParts", buildParts.toArray,
      s"$partitionClass[][]")
    val allIterators = ctx.freshName("allIterators")
    val indexVar = ctx.freshName("index")
    val contextName = ctx.freshName("context")
    val taskContextClass = classOf[TaskContext].getName
    ctx.addMutableState(taskContextClass, contextName,
      s"this.$contextName = $taskContextClass.get();")


    // switch inputs to use the buildPlan RDD iterators
    ctx.addMutableState("scala.collection.Iterator[]", allIterators,
      s"""
         |$allIterators = inputs;
         |inputs = new scala.collection.Iterator[$buildRDDs.length];
         |$taskContextClass $contextName = $taskContextClass.get();
         |for (int $indexVar = 0; $indexVar < $buildRDDs.length; $indexVar++) {
         |  $partitionClass[] parts = $buildPartsVar[$indexVar];
         |  // check for replicate table
         |  if (parts.length == 1) {
         |    inputs[$indexVar] = $buildRDDs[$indexVar].iterator(
         |      parts[0], $contextName);
         |  } else {
         |    inputs[$indexVar] = $buildRDDs[$indexVar].iterator(
         |      parts[partitionIndex], $contextName);
         |  }
         |}
      """.stripMargin)

    val buildProduce = buildCodeGen.produce(ctx, mapAccessor)
    // switch inputs back to streamPlan iterators
    val numIterators = ctx.freshName("numIterators")
    ctx.addMutableState("int", numIterators, s"inputs = $allIterators;")

    val numKeyColumns = buildSideKeys.length
    val longLived = replicatedTableJoin
    val buildSideCreateMap =
      s"""$hashSetClassName $hashMapTerm = new $hashSetClassName(128, 0.6,
      $numKeyColumns, $longLived, scala.reflect.ClassTag$$.MODULE$$.apply(
        $entryClass.class));
      this.$hashMapTerm = $hashMapTerm;
      int $maskTerm = $hashMapTerm.mask();
      $entryClass[] $mapDataTerm = ($entryClass[])$hashMapTerm.data();
      $buildProduce"""

    if (replicatedTableJoin) {
      var cacheClass = HashedObjectCache.getClass.getName
      cacheClass = cacheClass.substring(0, cacheClass.length - 1)
      ctx.addNewFunction(getOrCreateMap,
        s"""
        public final void $createMap() throws java.io.IOException {
          $buildSideCreateMap
        }

        public final void $getOrCreateMap() throws java.io.IOException {
          $hashMapTerm = $cacheClass.get($cacheKeyTerm, new $createMapClass(),
            $contextName, 1, scala.reflect.ClassTag$$.MODULE$$.apply($entryClass.class));
        }

        public final class $createMapClass implements java.util.concurrent.Callable {

          public Object call() throws java.io.IOException {
            $createMap();
            return $hashMapTerm;
          }
        }
      """)
    } else {
      ctx.addNewFunction(getOrCreateMap,
        s"""
          public final void $getOrCreateMap() throws java.io.IOException {
            $buildSideCreateMap
          }
      """)
    }

    // clear the parent by reflection if plan is serialized by operators like Sort
    TypeUtilities.parentSetter.invoke(buildPlan, null)

    // The child could change `copyResult` to true, but we had already
    // consumed all the rows, so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    // initialization of min/max for integral keys
    val initMinMaxVars = mapAccessor.integralKeys.zipWithIndex.map {
      case (indexKey, index) =>
        val minVar = mapAccessor.integralKeysMinVars(index)
        val maxVar = mapAccessor.integralKeysMaxVars(index)
        ctx.addMutableState("long", minVar, "")
        ctx.addMutableState("long", maxVar, "")
        s"""
          $minVar = $hashMapTerm.getMinValue($indexKey);
          $maxVar = $hashMapTerm.getMaxValue($indexKey);
        """
    }.mkString("\n")

    initMapCode =
        s"""
          final long $beforeMap = System.nanoTime();
          $getOrCreateMap();
          $buildTime.${metricAdd(s"(System.nanoTime() - $beforeMap) / 1000000")};

          this.$initMap = true;
          this.$mapSize = $hashMapTerm.size();
          this.$keyIsUniqueTerm = $keyIsUniqueTerm = $hashMapTerm.keyIsUnique();
          $initMinMaxVars
          this.$maskTerm = $maskTerm = $hashMapTerm.mask();
          this.$mapDataTerm = $mapDataTerm = ($entryClass[])$hashMapTerm.data();"""

    val produced = streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)

    s"""
      boolean $keyIsUniqueTerm = this.$keyIsUniqueTerm;
      int $maskTerm = this.$maskTerm;
      $entryClass[] $mapDataTerm = this.$mapDataTerm;
      long $numRowsTerm = 0L;
      try {
        ${session.evaluateFinallyCode(ctx, produced)}
      } finally {
        $numOutputRows.${metricAdd(numRowsTerm)};
      }
    """
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    // variable that holds if relation is unique to optimize iteration
    val entryVar = ctx.freshName("entry")
    val localValueVar = ctx.freshName("value")
    val checkNullObj = joinType match {
      case LeftOuter | RightOuter | FullOuter | LeftAnti => true
      case _ => false
    }
    val keyValueVars = mapAccessor.getColumnVars(entryVar, localValueVar,
      onlyKeyVars = false, onlyValueVars = false, checkNullObj)
    val buildKeyVars = keyValueVars.take(buildSideKeys.length)
    val buildVars = keyValueVars.drop(buildSideKeys.length)
    val checkCondition = getJoinCondition(ctx, input, buildVars)

    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val (resultVars, streamKeys) = buildSide match {
      case BuildLeft => (buildVars ++ input,
          streamSideKeys.map(BindReferences.bindReference(_, right.output)))
      case BuildRight => (input ++ buildVars,
          streamSideKeys.map(BindReferences.bindReference(_, left.output)))
    }
    val streamKeyVars = ctx.generateExpressions(streamKeys)

    mapAccessor.generateMapLookup(entryVar, localValueVar, mapSize, keyIsUniqueTerm,
      initMap, initMapCode, numRowsTerm, checkCondition, streamSideKeys,
      streamKeyVars, streamedPlan.output, buildKeyVars, buildVars, input,
      resultVars, dictionaryArrayTerm, dictionaryArrayInit, joinType, buildSide)
  }

  override def canConsume(plan: SparkPlan): Boolean = {
    // check for possible optimized dictionary code path;
    // below is a loose search while actual decision will be taken as per
    // availability of ExprCodeEx with DictionaryCode in doConsume
    DictionaryOptimizedMapAccessor.canHaveSingleKeyCase(streamSideKeys)
  }

  override def batchConsume(ctx: CodegenContext,
      plan: SparkPlan, input: Seq[ExprCode], numBatchRows: String): String = {
    if (!canConsume(plan)) return ""
    // create an empty method to populate the dictionary array
    // which will be actually filled with code in consume if the dictionary
    // optimization is possible using the incoming DictionaryCode
    val className = mapAccessor.getClassName
    // this array will be used at batch level for grouping if possible
    dictionaryArrayTerm = ctx.freshName("dictionaryArray")
    dictionaryArrayInit = ctx.freshName("dictionaryArrayInit")
    ctx.addNewFunction(dictionaryArrayInit,
      s"""
         |private $className[] $dictionaryArrayInit(int numBatchRows) {
         |  return null;
         |}
         """.stripMargin)
    s"final $className[] $dictionaryArrayTerm = $dictionaryArrayInit($numBatchRows);"
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows.
   * This is used in Inner joins.
   */
  private def getJoinCondition(ctx: CodegenContext,
      input: Seq[ExprCode],
      buildVars: Seq[ExprCode]): (Option[ExprCode], String, Option[Expression]) = condition match {
    case Some(expr) =>
      // evaluate the variables from build side used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars,
        expr.references)
      // filter the output via condition
      ctx.currentVars = input.map(_.copy(code = "")) ++ buildVars
      val ev = BindReferences.bindReference(expr,
        streamedPlan.output ++ buildPlan.output).genCode(ctx)
      (Some(ev), eval, condition)
    case None => (None, "", None)
  }
}

private[spark] final class CacheKey(private var exprIds: Array[Long],
    private var rddId: Int) extends Serializable with KryoSerializable {

  private[this] var hash: Int = {
    var h = 0
    val numIds = exprIds.length
    var i = 0
    while (i < numIds) {
      val id = exprIds(i)
      h = (h ^ 0x9e3779b9) + (id ^ (id >>> 32)).toInt + (h << 6) + (h >>> 2)
      i += 1
    }
    (h ^ 0x9e3779b9) + rddId + (h << 6) + (h >>> 2)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    val numIds = exprIds.length
    output.writeVarInt(numIds, true)
    var i = 0
    while (i < numIds) {
      output.writeLong(exprIds(i))
      i += 1
    }
    output.writeInt(rddId)
    output.writeInt(hash)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val numIds = input.readVarInt(true)
    val exprIds = new Array[Long](numIds)
    var i = 0
    while (i < numIds) {
      exprIds(i) = input.readLong()
      i += 1
    }
    this.exprIds = exprIds
    rddId = input.readInt()
    hash = input.readInt()
  }

  // noinspection HashCodeUsesVar
  override def hashCode(): Int = hash

  override def equals(obj: Any): Boolean = obj match {
    case other: CacheKey =>
      val exprIds = this.exprIds
      val otherExprIds = other.exprIds
      val numIds = exprIds.length
      if (rddId == other.rddId && numIds == otherExprIds.length) {
        var i = 0
        while (i < numIds) {
          if (exprIds(i) != otherExprIds(i)) return false
          i += 1
        }
        true
      } else false
    case _ => false
  }
}

object HashedObjectCache {

  private[this] val mapCache = CacheBuilder.newBuilder()
      .maximumSize(50)
      .build[CacheKey, (ObjectHashSet[_], AtomicInteger)]()

  @throws(classOf[IOException])
  def get[T <: AnyRef](key: CacheKey,
      callable: Callable[ObjectHashSet[T]], context: TaskContext,
      tries: Int)(tag: ClassTag[T]): ObjectHashSet[T] = {
    try {
      val cached = mapCache.get(key,
        new Callable[(ObjectHashSet[_], AtomicInteger)] {
          override def call(): (ObjectHashSet[_], AtomicInteger) = {
            (callable.call(), new AtomicInteger(0))
          }
        })
      // Increment reference and add reference removal at the end of this task.
      val counter = cached._2
      counter.incrementAndGet()
      // Do full removal if reference count goes down to zero. If any later
      // task requires it again after full removal, then it will create again.
      context.addTaskCompletionListener { _ =>
        if (counter.get() > 0 && counter.decrementAndGet() <= 0) {
          mapCache.invalidate(key)
          cached._1.asInstanceOf[ObjectHashSet[T]].freeStorageMemory()
        }
      }
      cached._1.asInstanceOf[ObjectHashSet[T]]
    } catch {
      case e: ExecutionException =>
        // in case of OOME from MemoryManager, try after clearing the cache
        val cause = e.getCause
        cause match {
          case _: OutOfMemoryError =>
            if (tries <= 10 && mapCache.size() > 0) {
              mapCache.invalidateAll()
              get(key, callable, context, tries + 1)(tag)
            } else {
              throw new IOException(cause.getMessage, cause)
            }
          case _ => throw new IOException(cause.getMessage, cause)
        }
      case e: Exception => throw new IOException(e.getMessage, e)
    }
  }

  def close(): Unit = {
    mapCache.invalidateAll()
  }
}
