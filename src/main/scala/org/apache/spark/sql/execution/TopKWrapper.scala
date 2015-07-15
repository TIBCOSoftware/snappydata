package org.apache.spark.sql.execution

import java.util.{Calendar, Date}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types.{DataType, StructField, StructType}


protected[sql] final class TopKWrapper(val name: String, val cms : CMSParams,
                                       val size: Int, val timeSeriesColumn: Int,
                                       val timeInterval: Long, val schema: StructType, val key: StructField,
                                       val frequencyCol: Option[StructField], val epoch: Long, val maxinterval : Int, val stsummary: Boolean)
  extends CastLongTime with Serializable {

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

  def apply(name: String, options: Map[String, Any],
            schema: StructType): TopKWrapper = {
    val keyTest = "key".ci
    val timeSeriesColumnTest = "timeSeriesColumn".ci
    val timeIntervalTest = "timeInterval".ci
    val depthTest = "depth".ci
    val widthTest = "width".ci
    val sizeTest = "size".ci
    val frequencyColTest = "frequencyCol".ci
    val epochTest = "epoch".ci
    val epsTest = "eps".ci
    val confidenceTest = "confidence".ci
    val streamSummary = "streamsummary".ci
    val maxinterval = "maxinterval".ci
    val cols = schema.fieldNames
    var epsAndcf = false;
    var widthAndDepth = false;
    var isstreamparam = false;
    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (key, depth, width, size, frequencyCol) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (key, tsCol, timeInterval, depth, width, size, frequencyCol, epoch, eps, confidence, stsummary, maxival) =
      options.foldLeft("", -1, 5L, 5, 200, 100, "", -1L, 0.01, 0.95, false, 20) {
        case ((k, ts, ti, cf, e, s, fr, ep, eps, cfi, su, mi), (opt, optV)) =>
          opt match {
            case keyTest() => (optV.toString, ts, ti, cf, e, s, fr, ep, eps, cfi, su, mi)
            case depthTest() =>
              widthAndDepth = true
              optV match {
                case fs: String => (k, ts, ti, fs.toInt, e, s, fr, ep, eps, cfi, su, mi)
                case fi: Int => (k, ts, ti, fi, e, s, fr, ep, eps, cfi, su, mi)
                case _ => throw new AnalysisException(
                  s"TopKCMS: Cannot parse int 'depth'=$optV")
              }
            case widthTest() =>
              widthAndDepth = true
              optV match {
                case fs: String => (k, ts, ti, cf, fs.toInt, s, fr, ep, eps, cfi, su, mi)
                case fi: Int => (k, ts, ti, cf, fi, s, fr, ep, eps, cfi, su, mi)
                case _ => throw new AnalysisException(
                  s"TopKCMS: Cannot parse int 'width'=$optV")
              }
            case timeSeriesColumnTest() => optV match {
              case tss: String => (k, columnIndex(tss, cols), ti, cf, e, s, fr, ep, eps, cfi, su, mi)
              case tsi: Int => (k, tsi, ti, cf, e, s, fr, ep, eps, cfi, su, mi)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse 'timeSeriesColumn'=$optV")
            }
            case timeIntervalTest() =>
              (k, ts, parseTimeInterval(optV, "TopKCMS"), cf, e, s, fr, ep, eps, cfi, su, mi)
            case sizeTest() => optV match {
              case si: Int => (k, ts, ti, cf, e, si, fr, ep, eps, cfi, su, mi)
              case ss: String => (k, ts, ti, cf, e, ss.toInt, fr, ep, eps, cfi, su, mi)
              case sl: Long => (k, ts, ti, cf, e, sl.toInt, fr, ep, eps, cfi, su, mi)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'size'=$optV")
            }
            case epochTest() => optV match {
              case si: Int => (k, ts, ti, cf, e, s, fr, si.toLong, eps, cfi, su, mi)
              case ss: String =>
                try {
                  (k, ts, ti, cf, e, s, fr, ss.toLong, eps, cfi, su, mi)
                } catch {
                  case nfe: NumberFormatException =>
                    try {
                      (k, ts, ti, cf, e, s, fr, CastLongTime.getMillis(
                        java.sql.Timestamp.valueOf(ss)), eps, cfi, su, mi)
                    } catch {
                      case iae: IllegalArgumentException =>
                        throw new AnalysisException(
                          s"TopKCMS: Cannot parse timestamp 'epoch'=$optV")
                    }
                }
              case sl: Long => (k, ts, ti, cf, e, s, fr, sl, eps, cfi, su, mi)
              case dt: Date => (k, ts, ti, cf, e, s, fr, dt.getTime, eps, cfi, su, mi)
              case cal: Calendar => (k, ts, ti, cf, e, s, fr, cal.getTimeInMillis, eps, cfi, su, mi)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'size'=$optV")
            }
            case frequencyColTest() => (k, ts, ti, cf, e, s, optV.toString, ep, eps, cfi, su, mi)
            case epsTest() =>
              epsAndcf = true
              optV match {
                case es: String => (k, ts, ti, cf, e, s, fr, ep, es.toDouble, cfi, su, mi)
                case ed: Double => (k, ts, ti, cf, e, s, fr, ep, ed, cfi, su, mi)
                case _ => throw new AnalysisException(
                  s"TopKCMS: Cannot parse double 'eps'=$optV")
              }
            case confidenceTest() =>
              epsAndcf = true
              optV match {
                case cfs: String => (k, ts, ti, cf, e, s, fr, ep, eps, cfs.toDouble, su, mi)
                case cfd: Double => (k, ts, ti, cf, e, s, fr, ep, eps, cfd, su, mi)
                case _ => throw new AnalysisException(
                  s"TopKCMS: Cannot parse double 'confidence'=$optV")
              }
            case maxinterval() =>
              isstreamparam = true
              optV match {
                case si: Int => (k, ts, ti, cf, e, s, fr, ep, eps, cfi, su, si)
                case ss: String => (k, ts, ti, cf, e, s, fr, ep, eps, cfi,  su, ss.toInt)
                case _ => throw new AnalysisException(
                  s"TopKCMS: Cannot parse int 'size'=$optV")
              }
            case streamSummary() => optV match {
              case sb: Boolean  => (k, ts, ti, cf, e, s, fr, ep, eps, cfi, sb, mi)
              case ss: String => (k, ts, ti, cf, e, s, fr, ep, eps, cfi,  ss.toBoolean, mi)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'size'=$optV")
            }
          }
      }
    if (epsAndcf && widthAndDepth)
      throw new AnalysisException("TopK parameters should specify either (eps, confidence) or (width, depth) and not both.")
    if ((epsAndcf || widthAndDepth) && stsummary)
      throw new AnalysisException("TopK parameters shouldn't specify hokusai params for a stream summary.")
    if (!stsummary && isstreamparam)
      throw new AnalysisException("TopK parameters shouldn't specify stream summary params for a Hokusai.")
    if (tsCol < 0 && isstreamparam)
        throw new AnalysisException("Timestamp column is required for stream summary")


      val cms = if (epsAndcf) CMSParams(eps, confidence) else CMSParams(width, depth)
    new TopKWrapper(name, cms, size, tsCol, timeInterval,
      schema, schema(key),
      if (frequencyCol.isEmpty) None else Some(schema(frequencyCol)), epoch, maxival, stsummary)
  }
}
