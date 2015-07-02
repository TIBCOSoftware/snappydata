package org.apache.spark.sql.sources

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, AnalysisException}
import org.apache.spark.sql.types._

/**
 * Cast a given column in a schema to epoch time in long milliseconds.
 */
trait CastLongTime {

  def timeColumnType: Option[DataType]

  def module: String

  /** Store type of column once to avoid checking for every row at runtime */
  protected final val castType: Int = {
    timeColumnType match {
      case None => -1
      case Some(LongType) => 0
      case Some(IntegerType) => 1
      case Some(TimestampType) | Some(DateType) => 2
      case Some(colType) => throw new AnalysisException(
        s"$module: Cannot parse time column having type $colType")
    }
  }

  protected final def parseMillisFromAny(ts: Any): Long = {
    ts match {
      case tts: java.sql.Timestamp =>
        val time = tts.getTime
        if (tts.getNanos >= 500000) time + 1 else time
      case td: java.util.Date => td.getTime
      case tl: Long => tl
      case ti: Int => ti.toLong
      case _ => throw new AnalysisException(
        s"$module: Cannot parse time column having type ${timeColumnType.get}")
    }
  }

  protected def getNullMillis(getDefaultForNull: Boolean) = -1L

  final def parseMillis(row: Row, timeCol: Int,
      getDefaultForNull: Boolean = false): Long = {
    try {
      castType match {
        case 0 =>
          val ts = row.getLong(timeCol)
          if (ts != 0 || !row.isNullAt(timeCol)) {
            ts
          } else {
            getNullMillis(getDefaultForNull)
          }
        case 1 =>
          val ts = row.getInt(timeCol)
          if (ts != 0 || !row.isNullAt(timeCol)) {
            ts.toLong
          } else {
            getNullMillis(getDefaultForNull)
          }
        case 2 =>
          val ts = row(timeCol)
          if (ts != null) {
            parseMillisFromAny(ts)
          } else {
            getNullMillis(getDefaultForNull)
          }
      }
    } catch {
      case NonFatal(e) => if (timeCol >= 0 && row.isNullAt(timeCol))
        getNullMillis(getDefaultForNull) else throw e
      case t => throw t
    }
  }
}
