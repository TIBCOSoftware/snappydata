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
package org.apache.spark.sql.collection

import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.sql.DriverManager

import scala.annotation.tailrec
import scala.collection.{mutable, Map => SMap}
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Sorting

import io.snappydata.ToolsCallback
import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.{Partition, Partitioner, SparkContext, SparkEnv, TaskContext}

object Utils {

  final val WEIGHTAGE_COLUMN_NAME = "SNAPPY_SAMPLER_WEIGHTAGE"
  final val SKIP_ANALYSIS_PREFIX = "SAMPLE_"

  // 1 - (1 - 0.95) / 2 = 0.975
  final val Z95Percent = new NormalDistribution().
      inverseCumulativeProbability(0.975)
  final val Z95Squared = Z95Percent * Z95Percent

  def fillArray[T](a: Array[_ >: T], v: T, start: Int, endP1: Int): Unit = {
    var index = start
    while (index < endP1) {
      a(index) = v
      index += 1
    }
  }

  def analysisException(msg: String,
      cause: Option[Throwable] = None): AnalysisException =
    new AnalysisException(msg, None, None, None, cause)

  def columnIndex(col: String, cols: Array[String], module: String): Int = {
    val colT = toUpperCase(col.trim)
    cols.indices.collectFirst {
      case index if col == cols(index) => index
      case index if colT == toUpperCase(cols(index)) => index
    }.getOrElse {
      throw analysisException(
        s"""$module: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
    }
  }

  def fieldName(f: StructField): String = {
    if (f.metadata.contains("name")) f.metadata.getString("name") else f.name
  }

  def getAllExecutorsMemoryStatus(
      sc: SparkContext): Map[BlockManagerId, (Long, Long)] = {
    val memoryStatus = sc.env.blockManager.master.getMemoryStatus
    // no filtering for local backend
    if (isLoner(sc)) memoryStatus else memoryStatus.filter(!_._1.isDriver)
  }

  def getHostExecutorId(blockId: BlockManagerId): String =
    TaskLocation.executorLocationTag + blockId.host + '_' + blockId.executorId

  def classForName(className: String): Class[_] =
    org.apache.spark.util.Utils.classForName(className)

  def ERROR_NO_QCS(module: String): String = s"$module: QCS is empty"

  def qcsOf(qa: Array[String], cols: Array[String],
      module: String): (Array[Int], Array[String]) = {
    val colIndexes = qa.map(columnIndex(_, cols, module))
    Sorting.quickSort(colIndexes)
    (colIndexes, colIndexes.map(cols))
  }

  def resolveQCS(qcsV: Option[Any], fieldNames: Array[String],
      module: String): (Array[Int], Array[String]) = {
    qcsV.map {
      case qi: Array[Int] => (qi, qi.map(fieldNames))
      case qs: String =>
        if (qs.isEmpty) throw analysisException(ERROR_NO_QCS(module))
        else qcsOf(qs.split(","), fieldNames, module)
      case qa: Array[String] => qcsOf(qa, fieldNames, module)
      case q => throw analysisException(
        s"$module: Cannot parse 'qcs'='$q'")
    }.getOrElse(throw analysisException(ERROR_NO_QCS(module)))
  }

  def matchOption(optName: String,
      options: SMap[String, Any]): Option[(String, Any)] = {
    val optionName = toLowerCase(optName)
    options.get(optionName).map((optionName, _)).orElse {
      options.collectFirst { case (key, value)
        if toLowerCase(key) == optionName => (key, value)
      }
    }
  }

  def resolveQCS(options: SMap[String, Any], fieldNames: Array[String],
      module: String): (Array[Int], Array[String]) = {
    resolveQCS(matchOption("qcs", options).map(_._2), fieldNames, module)
  }

  def parseInteger(v: Any, module: String, option: String, min: Int = 1,
      max: Int = Int.MaxValue): Int = {
    val vl: Long = v match {
      case vi: Int => vi
      case vs: String =>
        try {
          vs.toLong
        } catch {
          case nfe: NumberFormatException => throw analysisException(
            s"$module: Cannot parse int '$option' from string '$vs'")
        }
      case vl: Long => vl
      case vs: Short => vs
      case vb: Byte => vb
      case _ => throw analysisException(
        s"$module: Cannot parse int '$option'=$v")
    }
    if (vl >= min && vl <= max) {
      vl.toInt
    } else {
      throw analysisException(
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
          case nfe: NumberFormatException => throw analysisException(
            s"$module: Cannot parse double '$option' from string '$vs'")
        }
      case vf: Float => vf.toDouble
      case vi: Int => vi.toDouble
      case vl: Long => vl.toDouble
      case vn: Number => vn.doubleValue()
      case _ => throw analysisException(
        s"$module: Cannot parse double '$option'=$v")
    }
    if (exclusive) {
      if (vd > min && vd < max) {
        vd
      } else {
        throw analysisException(
          s"$module: Double value outside of bounds ($min-$max) '$option'=$vd")
      }
    }
    else if (vd >= min && vd <= max) {
      vd
    } else {
      throw analysisException(
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
      case _ => throw analysisException(
        s"$module: Cannot parse '$option'=$cv")
    }
    if (cl >= 0 && cl < cols.length) {
      cl.toInt
    } else {
      throw analysisException(s"$module: Column index out of bounds " +
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
        case _ => throw analysisException(
          s"$module: Cannot parse 'timeInterval': $tis")
      }
      case _ => throw analysisException(
        s"$module: Cannot parse 'timeInterval': $optV")
    }
  }

  def parseTimestamp(ts: String, module: String, col: String): Long = {
    try {
      ts.toLong
    } catch {
      case nfe: NumberFormatException =>
        try {
          CastLongTime.getMillis(java.sql.Timestamp.valueOf(ts))
        } catch {
          case iae: IllegalArgumentException =>
            throw analysisException(
              s"$module: Cannot parse timestamp '$col'=$ts")
        }
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
      f: (TaskContext, Partition) => Iterator[T], partitioner: Partitioner,
      numPartitions: Int): RDD[T] = {
    val cleanedF = sc.clean(f)
    new FixedPartitionRDD[T](sc, cleanedF, numPartitions, Some(partitioner))
  }

  def getInternalType(dataType: DataType): Class[_] = {
    dataType match {
      case ByteType => classOf[Byte]
      case IntegerType => classOf[Int]
      case LongType => classOf[Long]
      case FloatType => classOf[Float]
      case DoubleType => classOf[Double]
      case StringType => classOf[String]
      case DateType => classOf[Int]
      case TimestampType => classOf[Long]
      case d: DecimalType => classOf[Decimal]
      // case "binary" => org.apache.spark.sql.types.BinaryType
      // case "raw" => org.apache.spark.sql.types.BinaryType
      // case "logical" => org.apache.spark.sql.types.BooleanType
      // case "boolean" => org.apache.spark.sql.types.BooleanType
      case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
    }
  }

  @tailrec
  def getSQLDataType(dataType: DataType): DataType = dataType match {
    case udt: UserDefinedType[_] => getSQLDataType(udt.sqlType)
    case _ => dataType
  }

  def getClientHostPort(netServer: String): String = {
    val addrIdx = netServer.indexOf('/')
    val portEndIndex = netServer.indexOf(']')
    if (addrIdx > 0) {
      val portIndex = netServer.indexOf('[')
      netServer.substring(0, addrIdx) +
          netServer.substring(portIndex, portEndIndex + 1)
    } else {
      netServer.substring(addrIdx + 1, portEndIndex + 1)
    }
  }

  final def isLoner(sc: SparkContext): Boolean =
    sc.schedulerBackend.isInstanceOf[LocalSchedulerBackend]

  def toLowerCase(k: String): String = {
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

  def parseColumnsAsClob(s: String): (Boolean, Array[String]) = {
    if (s.trim.equals("*")) {
      (true, Array.empty[String])
    } else {
      (false, s.toUpperCase.split(','))
    }
  }

  def hasLowerCase(k: String): Boolean = {
    var index = 0
    val len = k.length
    while (index < len) {
      if (Character.isLowerCase(k.charAt(index))) {
        return true
      }
      index += 1
    }
    false
  }

  def toUpperCase(k: String): String = {
    var index = 0
    val len = k.length
    while (index < len) {
      if (Character.isLowerCase(k.charAt(index))) {
        return k.toUpperCase(java.util.Locale.ENGLISH)
      }
      index += 1
    }
    k
  }

  def schemaFields(schema: StructType): Map[String, StructField] = {
    Map(schema.fields.flatMap { f =>
      val name = if (f.metadata.contains("name")) f.metadata.getString("name")
      else f.name
      Iterator((name, f))
    }: _*)
  }

  def getSchemaFields(schema: StructType): Map[String, StructField] = {
    Map(schema.fields.flatMap { f =>
      Iterator((f.name, f))
    }: _*)
  }


  def getFields(o: Any): Map[String, Any] = {
    val fieldsAsPairs = for (field <- o.getClass.getDeclaredFields) yield {
      field.setAccessible(true)
      (field.getName, field.get(o))
    }
    Map(fieldsAsPairs: _*)
  }

  /**
    * Get the result schema given an optional explicit schema and base table.
    * In case both are specified, then check compatibility between the two.
    */
  def getSchemaAndPlanFromBase(schemaOpt: Option[StructType],
      baseTableOpt: Option[String], catalog: SnappyStoreHiveCatalog,
      asSelect: Boolean, table: String,
      tableType: String): (StructType, Option[LogicalPlan]) = {
    schemaOpt match {
      case Some(s) => baseTableOpt match {
        case Some(baseTableName) =>
          // if both baseTable and schema have been specified, then both
          // should have matching schema
          try {
            val tablePlan = catalog.lookupRelation(
              catalog.newQualifiedTableName(baseTableName))
            val tableSchema = tablePlan.schema
            if (catalog.compatibleSchema(tableSchema, s)) {
              (s, Some(tablePlan))
            } else if (asSelect) {
              throw analysisException(s"CREATE $tableType TABLE AS SELECT:" +
                  " mismatch of schema with that of base table." +
                  "\n  Base table schema: " + tableSchema +
                  "\n  AS SELECT schema: " + s)
            } else {
              throw analysisException(s"CREATE $tableType TABLE:" +
                  " mismatch of specified schema with that of base table." +
                  "\n  Base table schema: " + tableSchema +
                  "\n  Specified schema: " + s)
            }
          } catch {
            // continue with specified schema if base table fails to load
            case _: AnalysisException => (s, None)
          }
        case None => (s, None)
      }
      case None => baseTableOpt match {
        case Some(baseTable) =>
          try {
            // parquet and other such external tables may have different
            // schema representation so normalize the schema
            val tablePlan = catalog.lookupRelation(
              catalog.newQualifiedTableName(baseTable))
            (catalog.normalizeSchema(tablePlan.schema), Some(tablePlan))
          } catch {
            case ae: AnalysisException =>
              throw analysisException(s"Base table $baseTable " +
                  s"not found for $tableType TABLE $table", Some(ae))
          }
        case None =>
          throw analysisException(s"CREATE $tableType TABLE must provide " +
              "either column definition or baseTable option.")
      }
    }
  }

  def dataTypeStringBuilder(dataType: DataType,
      result: StringBuilder): Any => Unit = value => {
    dataType match {
      case TimestampType => value match {
        case l: Long => result.append(DateTimeUtils.toJavaTimestamp(l))
        case _ => result.append(value)
      }
      case DateType => value match {
        case i: Int => result.append(DateTimeUtils.toJavaDate(i))
        case _ => result.append(value)
      }
      case ArrayType(elementType, _) => value match {
        case data: ArrayData =>
          result.append('[')
          val len = data.numElements()
          if (len > 0) {
            val elementBuilder = dataTypeStringBuilder(elementType, result)
            elementBuilder(data.get(0, elementType))
            var index = 1
            while (index < len) {
              result.append(", ")
              elementBuilder(data.get(index, elementType))
              index += 1
            }
          }
          result.append(']')

        case _ => result.append(value)
      }
      case MapType(keyType, valueType, _) => value match {
        case data: MapData =>
          result.append('[')
          val len = data.numElements()
          if (len > 0) {
            val keyBuilder = dataTypeStringBuilder(keyType, result)
            val valueBuilder = dataTypeStringBuilder(valueType, result)
            val keys = data.keyArray()
            val values = data.valueArray()
            keyBuilder(keys.get(0, keyType))
            result.append('=')
            valueBuilder(values.get(0, valueType))
            var index = 1
            while (index < len) {
              result.append(", ")
              keyBuilder(keys.get(index, keyType))
              result.append('=')
              valueBuilder(values.get(index, valueType))
              index += 1
            }
          }
          result.append(']')

        case _ => result.append(value)
      }
      case StructType(fields) => value match {
        case data: InternalRow =>
          result.append('[')
          val len = fields.length
          if (len > 0) {
            val e0type = fields(0).dataType
            dataTypeStringBuilder(e0type, result)(data.get(0, e0type))
            var index = 1
            while (index < len) {
              result.append(", ")
              val elementType = fields(index).dataType
              dataTypeStringBuilder(elementType, result)(
                data.get(index, elementType))
              index += 1
            }
          }
          result.append(']')

        case _ => result.append(value)
      }
      case udt: UserDefinedType[_] =>
        // check if serialized
        if (value != null && !udt.userClass.isInstance(value)) {
          result.append(udt.deserialize(value))
        } else {
          result.append(value)
        }
      case _ => result.append(value)
    }
  }

  def getDriverClassName(url: String): String = DriverManager.getDriver(url) match {
    case wrapper: DriverWrapper => wrapper.wrapped.getClass.getCanonicalName
    case driver => driver.getClass.getCanonicalName
  }

  /**
    * Register given driver class with Spark's loader.
    */
  def registerDriver(driver: String): Unit = {
    try {
      DriverRegistry.register(driver)
    } catch {
      case cnfe: ClassNotFoundException => throw new IllegalArgumentException(
        s"Couldn't find driver class $driver", cnfe)
    }
  }

  /**
    * Register driver for given JDBC URL and return the driver class name.
    */
  def registerDriverUrl(url: String): String = {
    val driver = getDriverClassName(url)
    registerDriver(driver)
    driver
  }

  def withNewExecutionId[T](session: SparkSession,
      queryExecution: QueryExecution)(body: => T): T = {
    SQLExecution.withNewExecutionId(session, queryExecution)(body)
  }

  def immutableMap[A, B](m: mutable.Map[A, B]): Map[A, B] = new Map[A, B] {

    private[this] val map = m

    override def size = map.size

    override def -(elem: A) = {
      if (map.contains(elem)) {
        val builder = Map.newBuilder[A, B]
        for (pair <- map) if (pair._1 != elem) {
          builder += pair
        }
        builder.result()
      } else this
    }

    override def +[B1 >: B](kv: (A, B1)): Map[A, B1] = {
      val builder = Map.newBuilder[A, B1]
      val newKey = kv._1
      for (pair <- map) if (pair._1 != newKey) {
        builder += pair
      }
      builder += kv
      builder.result()
    }

    override def iterator = map.iterator

    override def foreach[U](f: ((A, B)) => U) = map.foreach(f)

    override def get(key: A) = map.get(key)
  }

  def createScalaConverter(dataType: DataType): Any => Any =
    CatalystTypeConverters.createToScalaConverter(dataType)

  def createCatalystConverter(dataType: DataType): Any => Any =
    CatalystTypeConverters.createToCatalystConverter(dataType)

  def getGenericRowValues(row: GenericRow): Array[Any] = row.values

  def newChunkedByteBuffer(chunks: Array[ByteBuffer]): ChunkedByteBuffer =
    new ChunkedByteBuffer(chunks)
}

class ExecutorLocalRDD[T: ClassTag](_sc: SparkContext,
    f: (TaskContext, ExecutorLocalPartition) => Iterator[T])
    extends RDD[T](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val numberedPeers = Utils.getAllExecutorsMemoryStatus(sparkContext).
        keySet.toList.zipWithIndex

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
    if (part.blockId.host != thisBlockId.host ||
        part.blockId.executorId != thisBlockId.executorId) {
      throw new IllegalStateException(
        s"Unexpected execution of $part on $thisBlockId")
    }

    f(context, part)
  }
}

class FixedPartitionRDD[T: ClassTag](_sc: SparkContext,
    f: (TaskContext, Partition) => Iterator[T], numPartitions: Int,
    override val partitioner: Option[Partitioner])
    extends RDD[T](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    var i = 0

    Array.fill[Partition](numPartitions)({
      val x = i
      i += 1
      new Partition() {
        override def index: Int = x
      }
    })
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    f(context, split)
  }
}

class ExecutorLocalPartition(override val index: Int,
    val blockId: BlockManagerId) extends Partition {

  def hostExecutorId: String = Utils.getHostExecutorId(blockId)

  override def toString: String = s"ExecutorLocalPartition($index, $blockId)"
}

class MultiBucketExecutorPartition(override val index: Int,
    val buckets: Set[Int], val hostExecutorIds: Seq[String]) extends Partition {

  override def toString: String =
    s"MultiBucketExecutorPartition($index, $buckets, $hostExecutorIds)"
}


private[spark] case class NarrowExecutorLocalSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition) extends Serializable {

  @throws[java.io.IOException]
  private def writeObject(oos: ObjectOutputStream): Unit =
    org.apache.spark.util.Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      split = rdd.partitions(splitIndex)
      oos.defaultWriteObject()
    }
}

/**
  * Stores information about the narrow dependencies used by a StoreRDD.
  *
  * @param narrowDep maps to the dependencies variable in the parent RDD:
  *                  for each one to one dependency in dependencies,
  *                  narrowDeps has a NarrowExecutorLocalSplitDep (describing
  *                  the partition for that dependency) at the corresponding
  *                  index. The size of narrowDeps should always be equal to
  *                  the number of parents.
  */
private[spark] class CoGroupExecutorLocalPartition(
    idx: Int, val blockId: BlockManagerId,
    val narrowDep: Option[NarrowExecutorLocalSplitDep])
    extends Partition with Serializable {

  override val index: Int = idx

  def hostExecutorId: String = Utils.getHostExecutorId(blockId)

  override def toString: String =
    s"CoGroupExecutorLocalPartition($index, $blockId)"

  override def hashCode(): Int = idx
}

class ExecutorMultiBucketLocalShellPartition(override val index: Int,
    val buckets: mutable.HashSet[Int],
    val hostList: mutable.ArrayBuffer[(String, String)]) extends Partition {
  override def toString: String =
    s"ExecutorMultiBucketLocalShellPartition($index, $buckets, $hostList"
}

object ToolsCallbackInit extends Logging {
  final val toolsCallback = {
    try {
      val c = org.apache.spark.util.Utils.classForName(
        "io.snappydata.ToolsCallbackImpl$")
      val tc = c.getField("MODULE$").get(null).asInstanceOf[ToolsCallback]
      logInfo("toolsCallback initialized")
      tc
    } catch {
      case cnf: ClassNotFoundException =>
        logWarning("toolsCallback couldn't be INITIALIZED." +
            "DriverURL won't get published to others.")
        null
    }
  }
}
