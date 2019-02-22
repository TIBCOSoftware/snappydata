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
import io.snappydata.sql.catalog.{RelationInfo, SnappyExternalCatalog}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.columnar.{ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.{SQLContext, SaveMode, SnappyParserConsts, SnappySession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType

class DefaultSource extends ExternalSchemaRelationProvider with SchemaRelationProvider
    with DataSourceRegister with Logging {

  override def shortName(): String = SnappyParserConsts.OPLOG_SOURCE

  override def createRelation(
      sqlContext: SQLContext,
      options: Map[String, String],
      schema: StructType): BaseRelation = {

    val schemaString = getSchemaString(options) match {
      case Some(s) => s
      case None => "" // table may already exist
    }
    // table has already been checked for existence by callers if required so here mode is Ignore
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]

    val parameters = new CaseInsensitiveMutableHashMap(options)
    val tableName = parameters.get(SnappyExternalCatalog.DBTABLE_PROPERTY).get
    val fullTableName = ExternalStoreUtils.removeInternalProps(parameters)
    ExternalStoreUtils.getAndSetTotalPartitions(
      session, parameters, forManagedTable = true, forColumnTable = false)
    StoreUtils.getAndSetPartitioningAndKeyColumns(session, schema = null, parameters)
    val ddlExtension = StoreUtils.ddlExtensionString(
      parameters, isRowTable = true, isShadowTable = false)

    new OpLogFormatRelation(tableName, fullTableName, schema, null, sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      options: Map[String, String]): BaseRelation = {
    // TODO hmeka to check and handle schema when not provided
    createRelation(sqlContext, options, null)
  }
}
