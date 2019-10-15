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
package org.apache.spark.sql.internal

import io.snappydata.{HintName, QueryHint}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.JoinStrategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, InsertIntoTable, LogicalPlan, ResolvedHint}
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.row.RowTableScan
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, SparkPlan}
import org.apache.spark.sql.types.{LongType, StructType}

/**
 * Unlike Spark's InsertIntoTable this plan provides the count of rows
 * inserted as the output.
 */
final class Insert23(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
    extends InsertIntoTable(table, partition, child, overwrite, ifNotExists) {

  override def output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override def copy(table: LogicalPlan = table,
      partition: Map[String, Option[String]] = partition,
      child: LogicalPlan = child,
      overwrite: Boolean = overwrite,
      ifNotExists: Boolean = ifNotExists): Insert23 = {
    new Insert23(table, partition, child, overwrite, ifNotExists)
  }
}

/**
 * An extension to [[ResolvedHint]] to encapsulate any kind of hint rather
 * than just broadcast.
 */
class ResolvedPlanWithHints23(child: LogicalPlan,
    override val allHints: Map[QueryHint.Type, HintName.Type])
    extends ResolvedHint(child, HintInfo(JoinStrategy.hasBroadcastHint(allHints)))
        with LogicalPlanWithHints {

  override def productArity: Int = 3

  override def productElement(n: Int): Any = n match {
    case 0 => child
    case 1 => hints
    case 2 => allHints
  }
}

final class ColumnTableScan23(output: Seq[Attribute], dataRDD: RDD[Any],
    otherRDDs: Seq[RDD[InternalRow]], numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    baseRelation: PartitionedDataSourceScan, relationSchema: StructType,
    allFilters: Seq[Expression], schemaAttributes: Seq[AttributeReference],
    caseSensitive: Boolean, isSampleReservoirAsRegion: Boolean)
    extends ColumnTableScan(output, dataRDD, otherRDDs, numBuckets, partitionColumns,
      partitionColumnAliases, baseRelation, relationSchema, allFilters, schemaAttributes,
      caseSensitive, isSampleReservoirAsRegion) {

  override protected def doCanonicalize(): SparkPlan = if (isCanonicalizedPlan) this else {
    new ColumnTableScan23(output, dataRDD = null, otherRDDs = Nil, numBuckets,
      partitionColumns = Nil, partitionColumnAliases = Nil, baseRelation, relationSchema,
      allFilters = Nil, schemaAttributes = Nil, caseSensitive = false, isSampleReservoirAsRegion)
  }
}

final class RowTableScan23(output: Seq[Attribute], schema: StructType, dataRDD: RDD[Any],
    numBuckets: Int, partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]], table: String,
    baseRelation: PartitionedDataSourceScan, caseSensitive: Boolean)
    extends RowTableScan(output, schema, dataRDD, numBuckets, partitionColumns,
      partitionColumnAliases, table, baseRelation, caseSensitive) {

  override protected def doCanonicalize(): SparkPlan = if (isCanonicalizedPlan) this else {
    new RowTableScan23(output, schema, dataRDD = null, numBuckets, partitionColumns = Nil,
      partitionColumnAliases = Nil, table, baseRelation, caseSensitive = false)
  }
}
