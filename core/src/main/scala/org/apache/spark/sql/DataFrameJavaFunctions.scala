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
package org.apache.spark.sql

import java.util.Collections

import scala.collection.JavaConversions._

import io.snappydata.Constant

import org.apache.spark.SnappyJavaHelperUtils._

class DataFrameJavaFunctions(val df: DataFrame) {


  /**
   * Creates stratified sampled data from given DataFrame
   * {{{
   *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
   * }}}
   */
  def stratifiedSample(options: java.util.Map[String, Object]): SampleDataFrame = {
    snappy.snappyOperationsOnDataFrame(df).stratifiedSample(options.toMap)
  }

  /**
   * Creates a DataFrame for given time instant that will be used when
   * inserting into top-K structures.
   *
   * @param time the time instant of the DataFrame as millis since epoch
   * @return
   */
  def withTime(time: java.lang.Long): DataFrameWithTime =
    snappy.snappyOperationsOnDataFrame(df).withTime(time)

  /**
   * Append to an existing cache table.
   * Automatically uses #cacheQuery if not done already.
   */
  def appendToTempTableCache(tableName: String): Unit =
    snappy.snappyOperationsOnDataFrame(df).appendToTempTableCache(tableName)


 /* def errorStats(columnName: String,
      groupBy: java.util.Set[String] = Collections.emptySet()): MultiColumnOpenHashMap[StatCounter] =
    snappy.samplingOperationsOnDataFrame(df).errorStats(columnName, groupBy.toSet)
*/
  def errorEstimateAverage(columnName: String, confidence: JDouble,
      groupByColumns: java.util.Set[String]): java.util.Map[Row, Tuple4[JDouble, JDouble, JDouble, JDouble]] = {
    val groupedColumns =
       if (groupByColumns == null)
          Collections.emptySet()
       else
         groupByColumns

    val result = snappy.samplingOperationsOnDataFrame(df)
        .errorEstimateAverage(columnName, confidence, groupedColumns.toSet)

    result.mapValues(toJDouble(_))
  }


  def withError(error: JDouble,
      confidence: JDouble = Constant.DEFAULT_CONFIDENCE): DataFrame =
    snappy.convertToAQPFrame(df).withError(error, confidence)

}


