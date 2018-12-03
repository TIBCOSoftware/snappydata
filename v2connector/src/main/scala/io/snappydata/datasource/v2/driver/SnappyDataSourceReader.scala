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

package io.snappydata.datasource.v2.driver

import scala.collection.mutable.ArrayBuffer

import io.snappydata.datasource.v2.{EvaluateFilter, SnappyDataPartitioning, SnappyStatistics, V2Constants}

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, Statistics, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportPartitioning, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

// created on driver
abstract class SnappyDataSourceReader(options: DataSourceOptions,
    tableMetaData: SnappyTableMetaData)
    extends DataSourceReader with
    SupportsReportPartitioning with
    SupportsPushDownRequiredColumns with
    SupportsPushDownFilters with
    SupportsReportStatistics {

  // projected columns
  var projectedColumns: Option[StructType] = None
  var filtersPushedToSnappy: Option[Array[Filter]] = None
  var whereClause: String = _
  var whereClauseArgs: ArrayBuffer[Any] = _

  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  override def readSchema(): StructType = {
    projectedColumns.getOrElse(tableMetaData.schema)
  }


  /**
   * Applies column pruning w.r.t. the given requiredSchema.
   *
   * Implementation should try its best to prune the unnecessary columns or nested fields, but it's
   * also OK to do the pruning partially, e.g., a data source may not be able to prune nested
   * fields, and only prune top-level columns.
   *
   * Note that, data source readers should update {@link DataSourceReader#readSchema()} after
   * applying column pruning.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    // called by the engine to set projected columns so that our implementation can use those.
    // Implementation should return these in readSchema()
    if (requiredSchema.length > 0) projectedColumns = Option(requiredSchema)
  }

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // This method is passed all filters and is supposed to return unhandled filters
    // We will return all the filters as unhandled filters initially using pushFilters()
    // TODO: update the filters that can be pushed to Snappy
    val (predicateClause, predicateArgs) = EvaluateFilter.evaluateWhereClause(filters)
    whereClause = predicateClause
    whereClauseArgs = predicateArgs
    filtersPushedToSnappy = Option(filters)
    filters
  }

  /**
   * Returns the filters that are pushed in {@link #pushFilters(Filter[])}.
   * It's possible that there is no filters in the query and {@link #pushFilters(Filter[])}
   * is never called, empty array should be returned for this case.
   */
  override def pushedFilters(): Array[Filter] = {
    // looks like not much of use
//    filtersPushedToSnappy.getOrElse(Array.empty[Filter])
    Array.empty[Filter]
  }

  /**
   * Returns the output data partitioning that this reader guarantees.
   */
  override def outputPartitioning(): Partitioning = {
    new SnappyDataPartitioning(tableMetaData)
  }

  override def getStatistics(): Statistics = {
    new SnappyStatistics(options.get(V2Constants.TABLE_NAME).get())
  }
}

case class QueryConstructs(projections: StructType, filters: Option[Array[Filter]] = None,
    whereClause: String = "", whereClauseArgs: ArrayBuffer[Any])