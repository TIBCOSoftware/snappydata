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
package org.apache.spark.sql.execution.oplog.impl

import com.gemstone.gemfire.internal.cache.LocalRegion
import io.snappydata.Constant
import io.snappydata.sql.catalog.RelationInfo
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualNullSafe, EqualTo, Expression, SpecificInternalRow, TokenLiteral, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.PartitionedDataSourceScan
import org.apache.spark.sql.execution.columnar.{ConnectionType, ExternalStore}
import org.apache.spark.sql.execution.columnar.impl.ColumnDelta
import org.apache.spark.sql.hive.HiveClientUtil
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap

class OpLogFormatRelation(
    dbTableName: String,
    _table: String,
    _userSchema: StructType,
    _partitioningColumns: Seq[String],
    _context: SQLContext) extends BaseRelation with TableScan with Logging {
  var schema: StructType = _userSchema
  // TODO need sanity checks after split
  var schemaName: String = _table.split('.')(0)
  var tableName: String = _table.split('.')(1)

  def refreshTableSchema: Unit = {
    val sc = _context.sparkContext
    val catalog = HiveClientUtil.getOrCreateExternalCatalog(sc, sc.conf)
    schemaName = _table.split("\\.")(0)
    tableName = _table.split("\\.")(1)
    val catalogTable = catalog.getTable(schemaName, tableName)
    schema = catalogTable.schema
  }


  def columnBatchTableName(session: Option[SparkSession] = None): String = {

    schemaName + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  def scnTbl(tableName: String, requiredColumns: Array[String],
      filters: Array[Expression], prunePartitions: () => Int): (RDD[Any], Array[Int]) = {

    val fieldNames = new ObjectLongHashMap[String](schema.length)
    (0 until schema.length).foreach(i =>
      fieldNames.put(Utils.toLowerCase(schema(i).name), i + 1))
    val projection = null
    //      requiredColumns.map { c =>
    //      val index = fieldNames.get(Utils.toLowerCase(c))
    //      if (index == 0) Utils.analysisException(s"Column $c does not exist in $tableName")
    //      index.toInt
    //    }

    logWarning(s"1891; getcolumnbatchrdd schema is null ${_userSchema}")
    val snappySession = SparkSession.builder().getOrCreate().asInstanceOf[SnappySession]
    (new OpLogColumnRdd(snappySession, dbTableName, tableName, _userSchema, projection,
      filters, (filters eq null) || filters.length == 0, prunePartitions), projection)
  }


  override def sqlContext: SQLContext = _context

  override def buildScan(): RDD[Row] = {
    val externalColumnTableName = columnBatchTableName()
    scnTbl(externalColumnTableName, null, null, null)._1.asInstanceOf[RDD[Row]]
  }

}
