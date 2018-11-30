/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.datasource.v2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.sources._

object EvaluateFilter {

  def evaluateWhereClause(filters: Array[Filter]): (String, ArrayBuffer[Any]) = {
    val numFilters = filters.length
    val filterWhereArgs = new ArrayBuffer[Any](numFilters)

    // TODO: return pushed filters
    val pushedFilters = Array.empty[Filter]
    val filtersNotPushed = Array.empty[Filter]

    val filterWhereClause = if (numFilters > 0) {
      val sb = new StringBuilder().append(" WHERE ")
      val initLen = sb.length
      filters.foreach(f => compileFilter(f, sb, filterWhereArgs, sb.length > initLen))
      if (filterWhereArgs.nonEmpty) {
        sb.toString()
      } else ""
    } else ""
    (filterWhereClause, filterWhereArgs)
  }

  // below should exactly match ExternalStoreUtils.handledFilter
  private def compileFilter(f: Filter, sb: StringBuilder,
      args: ArrayBuffer[Any], addAnd: Boolean): Unit = f match {
    case EqualTo(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" = ?")
      args += value
    case LessThan(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" < ?")
      args += value
    case GreaterThan(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" > ?")
      args += value
    case LessThanOrEqual(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" <= ?")
      args += value
    case GreaterThanOrEqual(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" >= ?")
      args += value
    case StringStartsWith(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(s" LIKE $value%")
    case In(col, values) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" IN (")
      (1 until values.length).foreach(_ => sb.append("?,"))
      sb.append("?)")
      args ++= values
    case And(left, right) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append('(')
      compileFilter(left, sb, args, addAnd = false)
      sb.append(") AND (")
      compileFilter(right, sb, args, addAnd = false)
      sb.append(')')
    case Or(left, right) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append('(')
      compileFilter(left, sb, args, addAnd = false)
      sb.append(") OR (")
      compileFilter(right, sb, args, addAnd = false)
      sb.append(')')
    case _ => // no filter pushdown
  }

}
