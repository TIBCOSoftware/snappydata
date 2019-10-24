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
package io.snappydata.gemxd

import java.io.{CharArrayWriter, DataOutput}
import java.sql.SQLWarning

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.cache.CacheClosedException
import com.gemstone.gemfire.internal.shared.{ClientSharedUtils, Version}
import com.gemstone.gemfire.internal.{ByteArrayDataInput, InternalDataSerializer}
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.execution.LeadNodeExecutionObject
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLChar}
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.{Constant, Property, QueryHint}

import org.apache.spark.serializer.{KryoSerializerPool, StructTypeSerializer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.{TableIdentifier, expressions}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.sources.SamplingRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CachedDataFrame, Row, SnappyContext, SnappySession}
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.SnappyUtils
import org.apache.spark.{Logging, SparkEnv}

/**
 * Encapsulates a Spark execution for use in query routing from JDBC.
 */
class SparkSampleInsertExecuteImpl(val baseTable: String,
  rows: Seq[Row],
  val ctx: LeadNodeExecutionContext,
  senderVersion: Version) extends SparkSQLExecute with Logging {

  // spark context will be constructed by now as this will be invoked when
  // DRDA queries will reach the lead node

  if (Thread.currentThread().getContextClassLoader != null) {
    val loader = SnappyUtils.getSnappyStoreContextLoader(
      SparkSQLExecuteImpl.getContextOrCurrentClassLoader)
    Thread.currentThread().setContextClassLoader(loader)
  }

  private[this] val session = SnappySessionPerConnection
    .getSnappySessionForConnection(ctx.getConnId)

  if (ctx.getUserName != null && !ctx.getUserName.isEmpty) {
    session.conf.set(Attribute.USERNAME_ATTR, ctx.getUserName)
    session.conf.set(Attribute.PASSWORD_ATTR, ctx.getAuthToken)
  }

  override def packRows(msg: LeadNodeExecutorMsg,
    snappyResultHolder: SnappyResultHolder, execObject: LeadNodeExecutionObject): Unit = {
    val ti = session.tableIdentifier(baseTable)
    val catalog = session.sessionState.catalog
    val baseTableMetadata = catalog.getTableMetadata(ti)
    val schema = baseTableMetadata.schema
    val encoder = RowEncoder(schema)
    val ds = session.internalCreateDataFrame(session.sparkContext.parallelize(
      rows.map(encoder.toRow)), schema)

    // get sample tables tracked in catalog
    val aqpRelations = catalog.getSampleRelations(ti)
    val isLocalExecution = msg.isLocallyExecuted
    aqpRelations.foreach(_._1.asInstanceOf[SamplingRelation].insert(ds, false))
    msg.lastResult(snappyResultHolder)
  }

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit =
    SparkSQLExecuteImpl.serializeRows(out, hasMetadata, hdos)

  private lazy val (tableNames, nullability) = SparkSQLExecuteImpl.
    getTableNamesAndNullability(session, df.queryExecution.analyzed.output)

  def getColumnNames: Array[String] = {
    querySchema.fieldNames
  }

  private def getColumnTypes: Array[(Int, Int, Int)] =
    querySchema.map(f => {
      SparkSQLExecuteImpl.getSQLType(f.dataType, complexTypeAsJson,
        f.metadata, Utils.toLowerCase(f.name), allAsClob, columnsAsClob)
    }).toArray

  private def getColumnDataTypes: Array[DataType] =
    querySchema.map(_.dataType).toArray
}

