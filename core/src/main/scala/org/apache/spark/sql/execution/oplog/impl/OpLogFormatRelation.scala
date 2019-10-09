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

import java.util

import io.snappydata.Constant
import io.snappydata.recovery.RecoveryService

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

class OpLogFormatRelation(
    fqtn: String,
    _schema: StructType,
    _partitioningColumns: Seq[String],
    _context: SQLContext) extends BaseRelation with TableScan with Logging {
  val schema: StructType = _schema
  var schemaName: String = fqtn.split('.')(0)
  var tableName: String = fqtn.split('.')(1)

  def columnBatchTableName(session: Option[SparkSession] = None): String = {
    schemaName + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  def scnTbl(externalColumnTableName: String, requiredColumns: Array[String],
      filters: Array[Expression], prunePartitions: () => Int): (RDD[Any], Array[Int]) = {

    val fieldNames = new util.HashMap[String, Integer](schema.length)
    (0 until schema.length).foreach(i => fieldNames.put(schema(i).name.toLowerCase, i + 1))
    val projection = null
    val provider = RecoveryService.getProvider(fqtn)
    val snappySession = SparkSession.builder().getOrCreate().asInstanceOf[SnappySession]

    val tableSchemas = RecoveryService.schemaStructMap
    val versionMap = RecoveryService.versionMap
    val tableColIdsMap = RecoveryService.tableColumnIds
    (new OpLogRdd(snappySession, fqtn.toUpperCase(), externalColumnTableName, schema,
      provider, projection, filters, (filters eq null) || filters.length == 0,
      prunePartitions, tableSchemas, versionMap, tableColIdsMap), projection)
  }


  override def sqlContext: SQLContext = _context

  override def buildScan(): RDD[Row] = {
    val externalColumnTableName = columnBatchTableName()
    scnTbl(externalColumnTableName, null, null, null)._1.asInstanceOf[RDD[Row]]
  }

}
