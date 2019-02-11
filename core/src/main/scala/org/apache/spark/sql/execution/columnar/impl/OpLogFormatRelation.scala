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
package org.apache.spark.sql.execution.columnar.impl

import com.gemstone.gemfire.internal.cache.LocalRegion
import io.snappydata.Property
import io.snappydata.sql.catalog.{RelationInfo, SnappyExternalCatalog}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualNullSafe, EqualTo, Expression, SortDirection, SpecificInternalRow, TokenLiteral, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.{ColumnPutIntoExec, ConnectionType, ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BulkPutRelation, ConnectionProperties, JdbcExtendedUtils}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap

import scala.util.control.NonFatal

class OpLogFormatRelation(
                           _table: String,
                           _provider: String,
                           _mode: SaveMode,
                           _userSchema: StructType,
                           _schemaExtensions: String,
                           _ddlExtensionForShadowTable: String,
                           _origOptions: Map[String, String],
                           _externalStore: ExternalStore,
                           _partitioningColumns: Seq[String],
                           _context: SQLContext,
                           _relationInfo: (RelationInfo, Option[LocalRegion]) = null)
  extends BaseColumnFormatRelation(
    _table,
    _provider,
    _mode,
    _userSchema,
    _schemaExtensions,
    _ddlExtensionForShadowTable,
    _origOptions,
    _externalStore,
    _partitioningColumns,
    _context,
    _relationInfo) {


  override def buildUnsafeScan(requiredColumns: Array[String],
                               filters: Array[Expression]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    // Remove the update/delete key columns from RDD requiredColumns.
    // These will be handled by the ColumnTableScan directly.
    val columns = requiredColumns.filter(!_.startsWith(ColumnDelta.mutableKeyNamePrefix))
    logInfo("1891: 1 buildUnsafeScan")

    val (rdd, projection) = scanTable(table, columns, filters, () => -1)
    //    val partitionEvaluator = rdd match {
    //      case c: TestRdd => c.getPartitionEvaluator
    //      case s => s.asInstanceOf[SmartConnectorColumnRDD].getPartitionEvaluator
    //    }
    //    // select the rowId from row buffer for update/delete keys
    //    val numColumns = columns.length
    //    val rowBufferColumns = if (numColumns < requiredColumns.length) {
    //      val newColumns = java.util.Arrays.copyOf(columns, numColumns + 1)
    //      newColumns(numColumns) = StoreUtils.ROWID_COLUMN_NAME
    //      newColumns
    //    } else columns
    //    val zipped = buildRowBufferRDD(partitionEvaluator, rowBufferColumns, filters,
    //      useResultSet = true, projection).zipPartitions(rdd) { (leftItr, rightItr) =>
    //      Iterator[Any](leftItr, rightItr)
    //    }
    //    (zipped, Nil)
    (rdd, Nil)
  }

  override def scanTable(tableName: String, requiredColumns: Array[String],
                         filters: Array[Expression], _ignore: () => Int): (RDD[Any], Array[Int]) = {

    // this will yield partitioning column ordered Array of Expression (Literals/ParamLiterals).
    // RDDs needn't have to care for orderless hashing scheme at invocation point.
    val (pruningExpressions, fields) = partitionColumns.map { pc =>
      filters.collectFirst {
        case EqualTo(a: Attribute, v) if TokenLiteral.isConstant(v) &&
          pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
        case EqualTo(v, a: Attribute) if TokenLiteral.isConstant(v) &&
          pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
        case EqualNullSafe(a: Attribute, v) if TokenLiteral.isConstant(v) &&
          pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
        case EqualNullSafe(v, a: Attribute) if TokenLiteral.isConstant(v) &&
          pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
      }
    }.filter(_.nonEmpty).map(_.get).unzip

    def prunePartitions(): Int = {
      val pcFields = StructType(fields).toAttributes
      val mutableRow = new SpecificInternalRow(pcFields.map(_.dataType))
      val bucketIdGeneration = UnsafeProjection.create(
        HashPartitioning(pcFields, numBuckets)
          .partitionIdExpression :: Nil, pcFields)
      if (pruningExpressions.nonEmpty &&
        // verify all the partition columns are provided as filters
        pruningExpressions.length == partitioningColumns.length) {
        pruningExpressions.zipWithIndex.foreach { case (e, i) =>
          mutableRow(i) = e.eval(null)
        }
        bucketIdGeneration(mutableRow).getInt(0)
      } else {
        -1
      }
    }

    logInfo("1891: 2 testformatrelation")
    // note: filters is expected to be already split by CNF.
    // see PhysicalScan#unapply
    scnTbl(externalColumnTableName, requiredColumns, filters, prunePartitions)

  }

  def scnTbl(tableName: String, requiredColumns: Array[String],
             filters: Array[Expression], prunePartitions: () => Int): (RDD[Any], Array[Int]) = {

    val fieldNames = new ObjectLongHashMap[String](schema.length)
    (0 until schema.length).foreach(i =>
      fieldNames.put(Utils.toLowerCase(schema(i).name), i + 1))
    val projection = requiredColumns.map { c =>
      val index = fieldNames.get(Utils.toLowerCase(c))
      if (index == 0) Utils.analysisException(s"Column $c does not exist in $tableName")
      index.toInt
    }
    logInfo("1891: 3 scntbl")
    readLock {
      (getColumnBatchRDD(tableName, rowBuffer = table, projection,
        filters, prunePartitions, sqlContext.sparkSession, schema, delayRollover), projection)
    }
  }

  def getColumnBatchRDD(tableName: String,
                        rowBuffer: String,
                        projection: Array[Int],
                        filters: Array[Expression],
                        prunePartitions: () => Int,
                        session: SparkSession,
                        schema: StructType,
                        delayRollover: Boolean): RDD[Any] = {
    val snappySession = session.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        logInfo(s"1891: 4 getColumnBatchRDD tablename  ${tableName}")
        val rdd = new OpLogColumnRdd(snappySession, tableName, schema, projection,
          filters, (filters eq null) || filters.length == 0, prunePartitions)
        rdd
      case _ =>
        // TODO add implementation
        null
    }
  }
}
