package org.apache.spark.sql

import org.apache.spark.partial.StudentTCacher
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.sql.types.{DoubleType, NumericType}
import org.apache.spark.util.StatCounter

import scala.collection.{Map => SMap}
import scala.util.Sorting

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

  // TODO: concurrency of the catalog?

  def registerSampleTable(tableName: String): Unit =
    sqlContext.catalog.registerSampleTable(schema, tableName,
      logicalPlan.options, Some(this))

  override def registerTempTable(tableName: String): Unit =
    registerSampleTable(tableName)

  def appendToCache(tableName: String): Unit =
    sqlContext.appendToCache(this, tableName)

  def errorStats(columnName: String): MultiColumnOpenHashMap[StatCounter] = {
    val schema = this.schema
    val columnIndex = SampleDataFrame.columnIndex(columnName,
      schema.fieldNames)
    val requireConversion = schema(columnIndex).dataType match {
      case dbl: DoubleType => false
      case numeric: NumericType => true // conversion required
      case tp => throw new AnalysisException("errorEstimateStats: Cannot " +
        s"estimate for non-integral column $columnName with type $tp")
    }

    mapPartitions { rows =>
      // group by qcs columns first
      //val qcsMap = new OpenHashMap[Row, StatCounter]
      val qcsMap = new MultiColumnOpenHashMap[StatCounter](qcs,
        qcs.map(schema(_).dataType))
      rows.foreach { row =>
        if (!row.isNullAt(columnIndex)) {
          //val pr = SampleDataFrame.projectQCS(row, qcs)
          val pr = row
          val stat = qcsMap.get(pr).getOrElse {
            val sc = new StatCounter()
            qcsMap(pr) = sc
            sc
          }
          if (requireConversion) {
            stat.merge(row(columnIndex).asInstanceOf[Number].doubleValue())
          }
          else {
            stat.merge(row.getDouble(columnIndex))
          }
        }
      }
      Iterator(qcsMap)
    }.reduce((map1, map2) => {
      // use larger of the two maps
      val (m1, m2) =
        if (map1.size >= map2.size) (map1, map2) else (map2, map1)
      m2.iteratorRowReuse.foreach { case (row, stat) =>
        // merge the two stats or just copy from m2 if m1 does not have the row
        m1.get(row).map(_.merge(stat)).getOrElse(m1.update(row, stat))
      }
      m1
    })
  }

  def errorEstimateAverage(columnName: String, confidence: Double) = {
    assert(confidence >= 0.0 && confidence <= 1.0,
      "confidence argument expected to be between 0.0 and 1.0")
    val tcache = new StudentTCacher(confidence)
    val stats = errorStats(columnName)
    stats.mapValues { stat =>
      val nsamples = stat.count
      val mean = stat.mean
      val stdev = math.sqrt(stat.sampleVariance / nsamples)
      val errorEstimate = tcache.get(nsamples) * stdev
      val percentError = (errorEstimate * 100.0) / math.abs(mean)
      (mean, stdev, errorEstimate, percentError)
    }
  }
}

object SampleDataFrame {

  final val WEIGHTAGE_COLUMN_NAME = "__STRATIFIED_SAMPLER_WEIGHTAGE"
  final val ERROR_NO_QCS = "StratifiedSampler: QCS is empty"

  def columnIndex(col: String, cols: Array[String]) = {
    val colT = col.trim
    cols.indices.collectFirst {
      case index if colT.equalsIgnoreCase(cols(index)) => index
    }.getOrElse {
      throw new AnalysisException(
        s"""StratifiedSampler: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
    }
  }

  def qcsOf(qa: Array[String], cols: Array[String]): Array[Int] = {
    val colIndexes = qa.map {
      columnIndex(_, cols)
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

  def projectQCS(row: Row, qcs: Array[Int]) = {
    val ncols = qcs.length
    val newRow = new Array[Any](ncols)
    var index = 0
    while (index < ncols) {
      newRow(index) = row(qcs(index))
      index += 1
    }
    new GenericRow(newRow)
  }
}
