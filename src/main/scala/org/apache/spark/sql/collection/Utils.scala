package org.apache.spark.sql.collection

import scala.collection.generic.{CanBuildFrom, MutableMapFactory}
import scala.collection.{Map => SMap, Traversable, mutable}
import scala.reflect.ClassTag
import scala.util.Sorting

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, AnalysisException, Row}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext, Partitioner}

object Utils extends MutableMapFactory[mutable.HashMap] {

  final val WEIGHTAGE_COLUMN_NAME = "__STRATIFIED_SAMPLER_WEIGHTAGE"

  // 1 - (1 - 0.95) / 2 = 0.975
  final val Z95Percent = new NormalDistribution().
      inverseCumulativeProbability(0.975)
  final val Z95Squared = Z95Percent * Z95Percent

  implicit class StringExtensions(val s: String) extends AnyVal {
    def normalize = normalizeId(s)
  }

  def fillArray[T](a: Array[_ >: T], v: T, start: Int, endP1: Int) = {
    var index = start
    while (index < endP1) {
      a(index) = v
      index += 1
    }
  }

  def columnIndex(col: String, cols: Array[String], module: String): Int = {
    val colT = normalizeId(col.trim)
    cols.indices.collectFirst {
      case index if colT == normalizeId(cols(index)) => index
    }.getOrElse {
      throw new AnalysisException(
        s"""$module: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
    }
  }

  def getAllExecutorsMemoryStatus(
      sc: SparkContext): Map[BlockManagerId, (Long, Long)] = {
    val memoryStatus = sc.env.blockManager.master.getMemoryStatus
    // no filtering for local backend
    sc.schedulerBackend match {
      case lb: LocalBackend => memoryStatus
      case _ => memoryStatus.filter(!_._1.isDriver)
    }
  }

  def getHostExecutorId(blockId: BlockManagerId) =
    blockId.host + '_' + blockId.executorId

  def ERROR_NO_QCS(module: String) = s"$module: QCS is empty"

  def qcsOf(qa: Array[String], cols: Array[String],
      module: String): Array[Int] = {
    val colIndexes = qa.map(columnIndex(_, cols, module))
    Sorting.quickSort(colIndexes)
    colIndexes
  }

  def resolveQCS(qcsV: Option[Any], fieldNames: Array[String],
      module: String): Array[Int] = {
    qcsV.map {
      case qi: Array[Int] => qi
      case qs: String =>
        if (qs.isEmpty) throw new AnalysisException(ERROR_NO_QCS(module))
        else qcsOf(qs.split(","), fieldNames, module)
      case qa: Array[String] => qcsOf(qa, fieldNames, module)
      case q => throw new AnalysisException(
        s"$module: Cannot parse 'qcs'='$q'")
    }.getOrElse(throw new AnalysisException(ERROR_NO_QCS(module)))
  }

  def matchOption(optName: String,
      options: SMap[String, Any]): Option[(String, Any)] = {
    val optionName = normalizeId(optName)
    options.get(optionName).map((optionName, _)).orElse {
      options.collectFirst { case (key, value)
        if normalizeId(key) == optionName => (key, value)
      }
    }
  }

  def resolveQCS(options: SMap[String, Any], fieldNames: Array[String],
      module: String): Array[Int] = {
    resolveQCS(matchOption("qcs", options).map(_._2), fieldNames, module)
  }

  def projectColumns(row: Row, columnIndices: Array[Int], schema: StructType,
      convertToScalaRow: Boolean) = {
    val ncols = columnIndices.length
    val newRow = new Array[Any](ncols)
    var index = 0
    if (convertToScalaRow) {
      while (index < ncols) {
        val colIndex = columnIndices(index)
        newRow(index) = CatalystTypeConverters.convertToScala(row(colIndex),
          schema(colIndex).dataType)
        index += 1
      }
    }
    else {
      while (index < ncols) {
        newRow(index) = row(columnIndices(index))
        index += 1
      }
    }
    new GenericRow(newRow)
  }

  def parseInteger(v: Any, module: String, option: String, min: Int = 1,
      max: Int = Int.MaxValue): Int = {
    val vl: Long = v match {
      case vi: Int => vi
      case vs: String =>
        try {
          vs.toLong
        } catch {
          case nfe: NumberFormatException => throw new AnalysisException(
            s"$module: Cannot parse int '$option' from string '$vs'")
        }
      case vl: Long => vl
      case vs: Short => vs
      case vb: Byte => vb
      case _ => throw new AnalysisException(
        s"$module: Cannot parse int '$option'=$v")
    }
    if (vl >= min && vl <= max) {
      vl.toInt
    } else {
      throw new AnalysisException(
        s"$module: Integer value outside of bounds [$min-$max] '$option'=$vl")
    }
  }

  def parseDouble(v: Any, module: String, option: String, min: Double,
      max: Double, exclusive: Boolean = true): Double = {
    val vd: Double = v match {
      case vd: Double => vd
      case vs: String =>
        try {
          vs.toDouble
        } catch {
          case nfe: NumberFormatException => throw new AnalysisException(
            s"$module: Cannot parse double '$option' from string '$vs'")
        }
      case vf: Float => vf.toDouble
      case vi: Int => vi.toDouble
      case vl: Long => vl.toDouble
      case vn: Number => vn.doubleValue()
      case _ => throw new AnalysisException(
        s"$module: Cannot parse double '$option'=$v")
    }
    if (exclusive) {
      if (vd > min && vd < max) {
        vd
      } else {
        throw new AnalysisException(
          s"$module: Double value outside of bounds ($min-$max) '$option'=$vd")
      }
    }
    else if (vd >= min && vd <= max) {
      vd
    } else {
      throw new AnalysisException(
        s"$module: Double value outside of bounds [$min-$max] '$option'=$vd")
    }
  }

  def parseColumn(cv: Any, cols: Array[String], module: String,
      option: String): Int = {
    val cl: Long = cv match {
      case cs: String => columnIndex(cs, cols, module)
      case ci: Int => ci
      case cl: Long => cl
      case cs: Short => cs
      case cb: Byte => cb
      case _ => throw new AnalysisException(
        s"$module: Cannot parse '$option'=$cv")
    }
    if (cl >= 0 && cl < cols.length) {
      cl.toInt
    } else {
      throw new AnalysisException(s"$module: Column index out of bounds " +
          s"for '$option'=$cl among ${cols.mkString(", ")}")
    }
  }

  /** string specification for time intervals */
  final val timeIntervalSpec = "([0-9]+)(ms|s|m|h)".r

  /**
   * Parse the given time interval value as long milliseconds.
   *
   * @see timeIntervalSpec for the allowed string specification
   */
  def parseTimeInterval(optV: Any, module: String): Long = {
    optV match {
      case tii: Int => tii.toLong
      case til: Long => til
      case tis: String => tis match {
        case timeIntervalSpec(interval, unit) =>
          unit match {
            case "ms" => interval.toLong
            case "s" => interval.toLong * 1000L
            case "m" => interval.toLong * 60000L
            case "h" => interval.toLong * 3600000L
            case _ => throw new AssertionError(
              s"unexpected regex match 'unit'=$unit")
          }
        case _ => throw new AnalysisException(
          s"$module: Cannot parse 'timeInterval'=$tis")
      }
      case _ => throw new AnalysisException(
        s"$module: Cannot parse 'timeInterval'=$optV")
    }
  }

  def mapExecutors[T: ClassTag](sqlContext: SQLContext,
      f: () => Iterator[T]): RDD[T] = {
    val sc = sqlContext.sparkContext
    val cleanedF = sc.clean(f)
    new ExecutorLocalRDD[T](sc,
      (context: TaskContext, part: ExecutorLocalPartition) => cleanedF())
  }

  def mapExecutors[T: ClassTag](sc: SparkContext,
      f: (TaskContext, ExecutorLocalPartition) => Iterator[T]): RDD[T] = {
    val cleanedF = sc.clean(f)
    new ExecutorLocalRDD[T](sc, cleanedF)
  }
  
  def getFixedPartitionRDD[T: ClassTag](sc: SparkContext,
      f: (TaskContext, Partition) => Iterator[T], partitioner: Partitioner, numPartitions: Int): RDD[T] = {
    val cleanedF = sc.clean(f)
    new FixedPartitionRDD[T](sc, cleanedF, numPartitions, Some(partitioner))
  }

  def normalizeId(k: String): String = {
    var index = 0
    val len = k.length
    while (index < len) {
      if (Character.isUpperCase(k.charAt(index))) {
        return k.toLowerCase(java.util.Locale.ENGLISH)
      }
      index += 1
    }
    k
  }

  def normalizeOptions[T](opts: Map[String, Any])
      (implicit bf: CanBuildFrom[Map[String, Any], (String, Any), T]): T =
    opts.map[(String, Any), T] {
      case (k, v) => (normalizeId(k), v)
    }(bf)

  // for mapping any tuple traversable to a mutable map

  def empty[A, B]: mutable.HashMap[A, B] = new mutable.HashMap[A, B]

  implicit def canBuildFrom[A, B] = new CanBuildFrom[Traversable[(A, B)],
      (A, B), mutable.HashMap[A, B]] {

    override def apply(from: Traversable[(A, B)]) = empty[A, B]

    override def apply() = empty
  }
}

class ExecutorLocalRDD[T: ClassTag](@transient _sc: SparkContext,
    f: (TaskContext, ExecutorLocalPartition) => Iterator[T])
    extends RDD[T](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val numberedPeers = Utils.getAllExecutorsMemoryStatus(sparkContext).
        keySet.zipWithIndex

    if (numberedPeers.nonEmpty) {
      numberedPeers.map {
        case (bid, idx) => createPartition(idx, bid)
      }.toArray[Partition]
    } else {
      Array.empty[Partition]
    }
  }

  def createPartition(index: Int,
      blockId: BlockManagerId): ExecutorLocalPartition =
    new ExecutorLocalPartition(index, blockId)

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val part = split.asInstanceOf[ExecutorLocalPartition]
    val thisBlockId = SparkEnv.get.blockManager.blockManagerId
    if (part.blockId != thisBlockId) {
      throw new IllegalStateException(
        s"Unexpected execution of $part on $thisBlockId")
    }

    f(context, part)
  }
}

class FixedPartitionRDD[T: ClassTag](@transient _sc: SparkContext,
  f: (TaskContext, Partition) => Iterator[T], numPartitions: Int, override val partitioner: Option[Partitioner])
  extends RDD[T](_sc, Nil) {
  @transient val partitionIDToExecutorMap = scala.collection.mutable.Map[Int, BlockManagerId ]()
 
  override def getPartitions: Array[Partition] = {
    var i = 0
    val blockIDs = Utils.getAllExecutorsMemoryStatus(sparkContext).keySet
    val blockIDsAsList = blockIDs.toList
    Array.fill[Partition](numPartitions)({
      val x = i
      i = i + 1
      val tempBlockID = this.partitionIDToExecutorMap.getOrElseUpdate(x, blockIDsAsList(x%blockIDsAsList.length))
      val blockID = if(blockIDs.contains(tempBlockID)) {
        tempBlockID
      }else {
        val newBlockIDs = blockIDs -- partitionIDToExecutorMap.values
        val newBid = if(!newBlockIDs.isEmpty) {
           newBlockIDs.iterator.next()          
        }else {
          blockIDsAsList(x%blockIDsAsList.length)
        }
        this.partitionIDToExecutorMap.update(x, newBid)
        newBid
      }
      new ExecutorLocalPartition(x,blockID) 
    })

  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    f(context, split)
  }
  
  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)
}

class FixedPartition(override val index: Int,
    val blockId: BlockManagerId) extends Partition {

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString = s"ExecutorLocalPartition($index, $blockId)"
}

class ExecutorLocalPartition(override val index: Int,
    val blockId: BlockManagerId) extends Partition {

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString = s"ExecutorLocalPartition($index, $blockId)"
}
