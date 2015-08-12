package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types.{ DataType, StructField, StructType }
import org.apache.spark.sql.Row
import org.apache.spark.Partitioner

protected[sql] final class TopKWrapper(val name: String, val cms: CMSParams,
  val size: Int, val timeSeriesColumn: Int,
  val timeInterval: Long, val schema: StructType, val key: StructField,
  val frequencyCol: Option[Int], val epoch: Long, val maxinterval: Int,
  val stsummary: Boolean) extends CastLongTime with Serializable {

  val rowToTupleConverter: (Row, Partitioner) => (Int, (Any, Any)) = TopKWrapper.getRowToTupleConverter(this)

  override protected def getNullMillis(getDefaultForNull: Boolean) =
    if (getDefaultForNull) System.currentTimeMillis() else -1L

  override def timeColumnType: Option[DataType] = {
    if (timeSeriesColumn >= 0) {
      Some(schema(timeSeriesColumn).dataType)
    } else {
      None
    }
  }

  override def module: String = "TopKHokusai"
}

object TopKWrapper {

  def apply(name: String, opts: Map[String, Any],
    schema: StructType): TopKWrapper = {

    val keyOpt = "key".normalize
    val depthOpt = "depth".normalize
    val widthOpt = "width".normalize
    val sizeOpt = "size".normalize
    val epsOpt = "eps".normalize
    val confidenceOpt = "confidence".normalize
    val streamSummaryOpt = "streamSummary".normalize
    val maxintervalOpt = "maxInterval".normalize
    val timeSeriesColumnOpt = "timeSeriesColumn".normalize
    val timeIntervalOpt = "timeInterval".normalize
    val frequencyColOpt = "frequencyCol".normalize
    val epochOpt = "epoch".normalize

    val cols = schema.fieldNames
    val module = "TopKCMS"

    var epsAndcf = false
    var widthAndDepth = false
    var isStreamParam = false

    // first normalize options into a mutable map to ease lookup and removal
    val options = normalizeOptions[mutable.HashMap[String, Any]](opts)

    val key = options.remove(keyOpt).map(_.toString).getOrElse("")

    val depth = options.remove(depthOpt).map { d =>
      widthAndDepth = true
      parseInteger(d, module, "depth")
    }.getOrElse(7)
    val width = options.remove(widthOpt).map { w =>
      widthAndDepth = true
      parseInteger(w, module, "width")
    }.getOrElse(200)
    val eps = options.remove(epsOpt).map { e =>
      epsAndcf = true
      parseDouble(e, module, "eps", 0.0, 1.0)
    }.getOrElse(0.01)
    val confidence = options.remove(confidenceOpt).map { c =>
      epsAndcf = true
      parseDouble(c, module, "confidence", 0.0, 1.0)
    }.getOrElse(0.95)

    val size = options.remove(sizeOpt).map(
      parseInteger(_, module, "size")).getOrElse(100)

    val stSummary = options.remove(streamSummaryOpt).exists {
      case sb: Boolean => sb
      case ss: String =>
        try {
          ss.toBoolean
        } catch {
          case iae: IllegalArgumentException =>
            throw new AnalysisException(
              s"$module: Cannot parse boolean 'streamSummary'=$ss")
        }
      case sv => throw new AnalysisException(
        s"TopKCMS: Cannot parse boolean 'streamSummary'=$sv")
    }
    val maxInterval = options.remove(maxintervalOpt).map { i =>
      isStreamParam = true
      parseInteger(i, module, "maxInterval")
    }.getOrElse(20)

    val tsCol = options.remove(timeSeriesColumnOpt).map(
      parseColumn(_, cols, module, "timeSeriesColumn")).getOrElse(-1)
    val timeInterval = options.remove(timeIntervalOpt).map(
      parseTimeInterval(_, module)).getOrElse(
        if (tsCol >= 0 || stSummary) 5000L else Long.MaxValue)

    val frequencyCol = options.remove(frequencyColOpt).map(
      parseColumn(_, cols, module, "frequencyCol"))

    val epoch = options.remove(epochOpt).map {
      case ei: Int => ei.toLong
      case el: Long => el
      case es: String =>
        try {
          es.toLong
        } catch {
          case nfe: NumberFormatException =>
            try {
              CastLongTime.getMillis(java.sql.Timestamp.valueOf(es))
            } catch {
              case iae: IllegalArgumentException =>
                throw new AnalysisException(
                  s"$module: Cannot parse timestamp 'epoch'=$es")
            }
        }
      case et: java.sql.Timestamp => CastLongTime.getMillis(et)
      case ed: java.util.Date => ed.getTime
      case ec: java.util.Calendar => ec.getTimeInMillis
      case en: Number => en.longValue()
      case ev => throw new AnalysisException(
        s"$module: Cannot parse int 'size'=$ev")
    }.getOrElse(-1L)

    // check for any remaining unsupported options
    if (options.nonEmpty) {
      val optMsg = if (options.size > 1) "options" else "option"
      throw new AnalysisException(
        s"$module: Unknown $optMsg: $options")
    }

    if (epsAndcf && widthAndDepth) {
      throw new AnalysisException("TopK parameters should specify either " +
        "(eps, confidence) or (width, depth) but not both.")
    }
    if ((epsAndcf || widthAndDepth) && stSummary) {
      throw new AnalysisException("TopK parameters shouldn't specify " +
        "hokusai params for a stream summary.")
    }
    if (isStreamParam) {
      if (!stSummary) {

        throw new AnalysisException("TopK parameters should specify " +
          "stream summary as true with stream summary params.")
      }
      if (tsCol < 0) {
        throw new AnalysisException(
          "Timestamp column is required for stream summary")
      }
    }

    val cms =
      if (epsAndcf) CMSParams(eps, confidence) else CMSParams(width, depth)

    new TopKWrapper(name, cms, size, tsCol, timeInterval,
      schema, schema(key), frequencyCol, epoch, maxInterval, stSummary)
  }

  private def getRowToTupleConverter(topkWrapper: TopKWrapper): (Row, Partitioner) => (Int, (Any, Any)) = {

    val tsCol = if (topkWrapper.timeInterval > 0)
      topkWrapper.timeSeriesColumn
    else -1

    val topKKeyIndex = topkWrapper.schema.fieldIndex(topkWrapper.key.name)
    if (tsCol < 0) {
      topkWrapper.frequencyCol match {
        case None =>
          (row: Row, partitioner: Partitioner) => partitioner.getPartition(row(topKKeyIndex)) -> (row(topKKeyIndex), null)
          case Some(freqCol) =>
          (row: Row, partitioner: Partitioner) => {
            val freq = row(freqCol) match {
              case num: Double => num.toLong
              case num: Float => num.toLong
              case num: java.lang.Double => num.longValue().toLong
              case num: java.lang.Float => num.longValue().toLong
              case num: java.lang.Integer => num.intValue().toLong
              case num: java.lang.Long => num.longValue().toLong
              case x => x
            }
            partitioner.getPartition(row(topKKeyIndex)) -> (row(topKKeyIndex), freq)
          }

      }
    } else {

      topkWrapper.frequencyCol match {
        case None =>
          (row: Row, partitioner: Partitioner) => {
            val key = row(topKKeyIndex)
            val timeVal = topkWrapper.parseMillis(row, tsCol)
            partitioner.getPartition(key) -> (key, timeVal)
          }

          case Some(freqCol) =>
          (row: Row, partitioner: Partitioner) => {

            val freq = row(freqCol) match {
              case num: Double => num.toLong
              case num: Float => num.toLong
              case num: java.lang.Double => num.longValue().toLong
              case num: java.lang.Float => num.longValue().toLong
              case num: java.lang.Integer => num.intValue().toLong
              case num: java.lang.Long => num.longValue().toLong
              case x => x
            }
            val key = row(topKKeyIndex)
            val timeVal = topkWrapper.parseMillis(row, tsCol)
            partitioner.getPartition(key) -> (key, (freq, timeVal))
          }
      }

    }

  }
}
