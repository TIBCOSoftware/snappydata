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
import java.sql.DriverManager

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map => SMap}
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Sorting

import io.snappydata.ToolsCallback
import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.storage.BlockManagerId

object Utils {

  final val WEIGHTAGE_COLUMN_NAME = "STRATIFIED_SAMPLER_WEIGHTAGE"

  // 1 - (1 - 0.95) / 2 = 0.975
  final val Z95Percent = new NormalDistribution().
      inverseCumulativeProbability(0.975)
  final val Z95Squared = Z95Percent * Z95Percent

  implicit class StringExtensions(val s: String) extends AnyVal {
    def normalize = toLowerCase(s)
  }

  def fillArray[T](a: Array[_ >: T], v: T, start: Int, endP1: Int) = {
    var index = start
    while (index < endP1) {
      a(index) = v
      index += 1
    }
  }

  def analysisException(msg: String): AnalysisException =
    new AnalysisException(msg)

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

  def getFields(cols: Array[String], schema: StructType,
      module: String) = {
    cols.map { col =>
      val colT = toUpperCase(col.trim)
      schema.fields.collectFirst {
        case field if colT == toUpperCase(field.name) => field
      }.getOrElse {
        throw analysisException(
          s"""$module: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
      }
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

  def getHostExecutorId(blockId: BlockManagerId) =
    TaskLocation.executorLocationTag + blockId.host + '_' + blockId.executorId

  def ERROR_NO_QCS(module: String) = s"$module: QCS is empty"

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

  def projectColumns(row: Row, columnIndices: Array[Int], schema: StructType,
      convertToScalaRow: Boolean): GenericRow = {
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
      //case "binary" => org.apache.spark.sql.types.BinaryType
      //case "raw" => org.apache.spark.sql.types.BinaryType
      //case "logical" => org.apache.spark.sql.types.BooleanType
      //case "boolean" => org.apache.spark.sql.types.BooleanType
      case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
    }
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
    sc.schedulerBackend.isInstanceOf[LocalBackend]

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

  def schemaFields(schema : StructType): Map[String, StructField] = {
    Map(schema.fields.flatMap { f =>
      val name = if (f.metadata.contains("name")) f.metadata.getString("name")
      else f.name
      Iterator((name, f))
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
              throw new DDLException(s"CREATE $tableType TABLE AS SELECT:" +
                  " mismatch of schema with that of base table." +
                  "\n  Base table schema: " + tableSchema +
                  "\n  AS SELECT schema: " + s)
            } else {
              throw new DDLException(s"CREATE $tableType TABLE:" +
                  " mismatch of specified schema with that of base table." +
                  "\n  Base table schema: " + tableSchema +
                  "\n  Specified schema: " + s)
            }
          } catch {
            // continue with specified schema if base table fails to load
            case e@(_: AnalysisException | _: DDLException) => (s, None)
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
            case e@(_: AnalysisException | _: DDLException) =>
              val ae = analysisException(s"Base table $baseTable " +
                  s"not found for $tableType TABLE $table")
              ae.initCause(e)
              throw ae
          }
        case None =>
          throw new DDLException(s"CREATE $tableType TABLE must provide " +
              "either column definition or baseTable option.")
      }
    }
  }

  def dataTypeStringBuilder(dataType: DataType,
      result: StringBuilder): Any => Unit = value => {
    dataType match {
      case utype: UserDefinedType[_] =>
        // check if serialized
        if (value != null && !utype.userClass.isInstance(value)) {
          result.append(utype.deserialize(value))
        } else {
          result.append(value)
        }
      case atype: ArrayType => value match {
        case data: ArrayData =>
          result.append('[')
          val etype = atype.elementType
          val len = data.numElements()
          if (len > 0) {
            val ebuilder = dataTypeStringBuilder(etype, result)
            ebuilder(data.get(0, etype))
            var index = 1
            while (index < len) {
              result.append(',')
              ebuilder(data.get(index, etype))
              index += 1
            }
          }
          result.append(']')

        case _ => result.append(value)
      }
      case mtype: MapType => value match {
        case data: MapData =>
          result.append('[')
          val ktype = mtype.keyType
          val vtype = mtype.valueType
          val len = data.numElements()
          if (len > 0) {
            val kbuilder = dataTypeStringBuilder(ktype, result)
            val vbuilder = dataTypeStringBuilder(vtype, result)
            val keys = data.keyArray()
            val values = data.valueArray()
            kbuilder(keys.get(0, ktype))
            result.append('=')
            vbuilder(values.get(0, ktype))
            var index = 1
            while (index < len) {
              result.append(',')
              kbuilder(keys.get(index, ktype))
              result.append('=')
              vbuilder(values.get(index, ktype))
              index += 1
            }
          }
          result.append(']')

        case _ => result.append(value)
      }
      case stype: StructType => value match {
        case data: InternalRow =>
          result.append('[')
          val len = data.numFields
          if (len > 0) {
            val etype = stype.fields(0).dataType
            dataTypeStringBuilder(etype, result)(data.get(0, etype))
            var index = 1
            while (index < len) {
              result.append(',')
              val etype = stype.fields(index).dataType
              dataTypeStringBuilder(etype, result)(data.get(index, etype))
              index += 1
            }
          }
          result.append(']')

        case _ => result.append(value)
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

  def withNewExecutionId[T](ctx: SQLContext,
      queryExecution: QueryExecution)(body: => T): T = {
    SQLExecution.withNewExecutionId(ctx, queryExecution)(body)
  }
}

class ExecutorLocalRDD[T: ClassTag](
    @transient private[this] val _sc: SparkContext,
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
    if (part.blockId.host != thisBlockId.host ||
        part.blockId.executorId != thisBlockId.executorId) {
      throw new IllegalStateException(
        s"Unexpected execution of $part on $thisBlockId")
    }

    f(context, part)
  }
}

class FixedPartitionRDD[T: ClassTag](@transient _sc: SparkContext,
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

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString = s"ExecutorLocalPartition($index, $blockId)"
}

class MultiExecutorLocalPartition(override val index: Int,
    val blockIds: Seq[BlockManagerId]) extends Partition {

  def hostExecutorIds = blockIds.map(blockId => Utils.getHostExecutorId(blockId))

  override def toString = s"MultiExecutorLocalPartition($index, $blockIds)"
}


private[spark] case class NarrowExecutorLocalSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
    ) extends Serializable {

  @throws[java.io.IOException]
  private def writeObject(oos: ObjectOutputStream): Unit = org.apache.spark.util.Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

/**
 * Stores information about the narrow dependencies used by a StoreRDD.
 *
 * @param narrowDep maps to the dependencies variable in the parent RDD: for each one to one
 *                   dependency in dependencies, narrowDeps has a NarrowExecutorLocalSplitDep (describing
 *                   the partition for that dependency) at the corresponding index. The size of
 *                   narrowDeps should always be equal to the number of parents.
 */
private[spark] class CoGroupExecutorLocalPartition(
    idx: Int, val blockId: BlockManagerId, val narrowDep: Option[NarrowExecutorLocalSplitDep])
    extends Partition with Serializable {

  override val index: Int = idx

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString = s"CoGroupExecutorLocalPartition($index, $blockId)"

  override def hashCode(): Int = idx
}

class ExecutorLocalShellPartition(override val index: Int,
    val hostList: ArrayBuffer[(String, String)]) extends Partition {
  override def toString = s"ExecutorLocalShellPartition($index, $hostList"
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
