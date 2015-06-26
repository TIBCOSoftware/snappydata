package org.apache.spark.sql

import scala.collection.{Map => SMap, mutable}
import scala.util.Sorting

import org.apache.spark.partial.StudentTCacher
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.collection.{MultiColumnOpenHashMap, Utils}
import org.apache.spark.sql.execution.StratifiedSample
import org.apache.spark.sql.types.{DoubleType, NumericType, StructType}
import org.apache.spark.util.StatCounter

/**
 * Encapsulates a DataFrame created after stratified sampling.
 *
 * Created by sumedh on 13/6/15.
 */
class SampleDataFrame(@transient override val sqlContext: SnappyContext,
    @transient override val logicalPlan: StratifiedSample)
    extends DataFrame(sqlContext, logicalPlan) with Serializable {

  /** LogicalPlan is deliberately transient, so keep qcs separately */
  final val qcs = logicalPlan.qcs

  final type ErrorRow = (Double, Double, Double, Double)

  // TODO: concurrency of the catalog?

  def registerSampleTable(tableName: String): Unit =
    sqlContext.catalog.registerSampleTable(schema, tableName,
      logicalPlan.options, Some(this))

  override def registerTempTable(tableName: String): Unit =
    registerSampleTable(tableName)

  def errorStats(columnName: String,
      groupBy: Set[String] = Set.empty): MultiColumnOpenHashMap[StatCounter] = {
    val schema = this.schema
    val allColumns = schema.fieldNames
    val columnIndex = Utils.columnIndex(columnName, allColumns)
    val requireConversion = schema(columnIndex).dataType match {
      case dbl: DoubleType => false
      case numeric: NumericType => true // conversion required
      case tp => throw new AnalysisException("errorEstimateStats: Cannot " +
          s"estimate for non-integral column $columnName with type $tp")
    }

    // map group by columns to indices
    val columnIndices = if (groupBy != null && groupBy.nonEmpty) {
      val groupByIndices = groupBy.map(Utils.columnIndex(_, allColumns))
      // check that all columns should be part of qcs
      val qcsCols = intArrayOps(logicalPlan.qcs)
      for (col <- groupByIndices) {
        require(qcsCols.indexOf(col) >= 0, "group by columns should be " +
            s"part of QCS: ${qcsCols.map(allColumns(_)).mkString(", ")}")
      }
      if (groupByIndices.size == qcs.length) qcs
      else groupByIndices.toSeq.sorted.toArray
    } else qcs

    mapPartitions { rows =>
      // group by column map
      val groupedMap = new MultiColumnOpenHashMap[StatCounter](columnIndices,
        columnIndices.map(schema(_).dataType))
      for (row <- rows) {
        if (!row.isNullAt(columnIndex)) {
          val stat = groupedMap.get(row).getOrElse {
            val sc = new StatCounter()
            groupedMap(row) = sc
            sc
          }
          // merge the new row into statistics
          if (requireConversion) {
            stat.merge(row(columnIndex).asInstanceOf[Number].doubleValue())
          }
          else {
            stat.merge(row.getDouble(columnIndex))
          }
        }
      }
      Iterator(groupedMap)
    }.reduce((map1, map2) => {
      // use larger of the two maps
      val (m1, m2) =
        if (map1.size >= map2.size) (map1, map2) else (map2, map1)
      if (m2.nonEmpty) {
        for ((row, stat) <- m2.iterator) {
          // merge the two stats or copy from m2 if m1 does not have the row
          m1.get(row) match {
            case Some(s) => s.merge(stat)
            case None => m1(row) = stat
          }
        }
      }
      m1
    })
  }

  def errorEstimateAverage(columnName: String, confidence: Double,
      groupByColumns: Set[String] = Set.empty): mutable.Map[Row, ErrorRow] = {
    assert(confidence >= 0.0 && confidence <= 1.0,
      "confidence argument expected to be between 0.0 and 1.0")
    val tcache = new StudentTCacher(confidence)
    val stats = errorStats(columnName)
    stats.mapValues { stat =>
      val nsamples = stat.count
      val mean = stat.mean
      val stdev = math.sqrt(stat.variance / nsamples)
      // 30 is taken to be cut-off limit in most statistics calculations
      // for z vs t distributions (unlike StudentTCacher that uses 100)
      val errorEstimate = if (nsamples >= 30) tcache.normalApprox * stdev
                          else tcache.get(nsamples) * stdev
      val percentError = (errorEstimate * 100.0) / math.abs(mean)
      (mean, stdev, errorEstimate, percentError)
    }
  }
}

object SampleDataFrame {

  final val WEIGHTAGE_COLUMN_NAME = "__STRATIFIED_SAMPLER_WEIGHTAGE"
  final val ERROR_NO_QCS = "StratifiedSampler: QCS is empty"

  def qcsOf(qa: Array[String], cols: Array[String]): Array[Int] = {
    val colIndexes = qa.map {
      Utils.columnIndex(_, cols)
    }
    Sorting.quickSort(colIndexes)
    colIndexes
  }

  def matchOption(optionName: String,
      options: SMap[String, Any]): Option[(String, Any)] = {
    options.get(optionName).map((optionName, _)).orElse {
      options.collectFirst { case (key, value)
        if key.equalsIgnoreCase(optionName) => (key, value)
      }
    }
  }

  def resolveQCS(options: SMap[String, Any], fieldNames: Array[String]) = {
    matchOption("qcs", options).getOrElse(
      throw new AnalysisException(ERROR_NO_QCS))._2 match {
      case qi: Array[Int] => qi
      case qs: String => qcsOf(qs.split(","), fieldNames)
      case qa: Array[String] => qcsOf(qa, fieldNames)
      case q => throw new AnalysisException(
        s"StratifiedSampler: Cannot parse 'qcs'='$q'")
    }
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
}
