/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, LogicalPlan}
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.row.{RowFormatScanRDD, RowTableScan}
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, SparkPlan}
import org.apache.spark.sql.types.StructType

/**
 * An extended version of [[BroadcastHint]] to encapsulate any kind of hint rather
 * than just broadcast.
 */
class PlanWithHints21(_child: LogicalPlan,
    val allHints: Map[QueryHint.Type, HintName.Type]) extends BroadcastHint(_child) {

  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => child
    case 1 => allHints
  }

  override def simpleString: String =
    s"PlanWithHints[hints = $allHints; child = ${child.simpleString}]"
}

final class ColumnTableScan21(output: Seq[Attribute], dataRDD: RDD[Any],
    otherRDDs: Seq[RDD[InternalRow]], numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    baseRelation: PartitionedDataSourceScan, relationSchema: StructType,
    allFilters: Seq[Expression], schemaAttributes: Seq[AttributeReference],
    caseSensitive: Boolean, isForSampleReservoirAsRegion: Boolean)
    extends ColumnTableScan(output, dataRDD, otherRDDs, numBuckets, partitionColumns,
      partitionColumnAliases, baseRelation, relationSchema, allFilters, schemaAttributes,
      caseSensitive, isForSampleReservoirAsRegion) {

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case r: ColumnTableScan => r.baseRelation.table == baseRelation.table &&
        r.numBuckets == numBuckets && r.schema == schema
    case _ => false
  }
}

final class RowTableScan21(output: Seq[Attribute], schema: StructType, dataRDD: RDD[Any],
    numBuckets: Int, partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]], table: String,
    baseRelation: PartitionedDataSourceScan, caseSensitive: Boolean)
    extends RowTableScan(output, schema, dataRDD, numBuckets, partitionColumns,
      partitionColumnAliases, table, baseRelation, caseSensitive) {

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case r: RowTableScan => r.table == table && r.numBuckets == numBuckets &&
        r.schema == schema && (this.dataRDD match {
      case rowRdd: RowFormatScanRDD =>
        val rdd2 = r.dataRDD.asInstanceOf[RowFormatScanRDD]
        rowRdd.filters.length == rdd2.filters.length &&
            rowRdd.filters.indices.forall(i => rowRdd.filters(i).semanticEquals(rdd2.filters(i)))
      case _ => true
    })
    case _ => false
  }
}
