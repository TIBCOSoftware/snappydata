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
package org.apache.spark.sql.sources

import scala.util.control.NonFatal

import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row}

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
      case tts: java.sql.Timestamp => CastLongTime.getMillis(tts)
      case td: java.util.Date => td.getTime
      case ts: String => CastLongTime.getMillis(java.sql.Timestamp.valueOf(ts))
      case tl: Long => tl
      case ti: Int => ti.toLong
      case _ => throw new AnalysisException(
        s"$module: Cannot parse time column having type ${timeColumnType.get}")
    }
  }

  protected def getNullMillis(getDefaultForNull: Boolean): Long = -1L

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
      case NonFatal(e) =>
        if (timeCol >= 0 && row.isNullAt(timeCol)) getNullMillis(getDefaultForNull)
        else throw e
    }
  }
}

object CastLongTime {

  final def getMillis(timestamp: java.sql.Timestamp): Long = {
    val time = timestamp.getTime
    if (timestamp.getNanos >= 500000) time + 1 else time
  }
}
