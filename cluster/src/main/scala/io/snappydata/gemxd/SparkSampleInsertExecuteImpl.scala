/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import java.io.DataOutput
import java.util

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.distributed.execution.LeadNodeExecutionObject
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.{DVDIOUtil, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SnappySessionCatalog
import org.apache.spark.sql.sources.SamplingRelation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.SnappyUtils

/**
 * Encapsulates a Spark execution for use in query routing from JDBC.
 */
class SparkSampleInsertExecuteImpl(val baseTable: String,
  dvdRows: util.List[Array[DataValueDescriptor]],
  serializedDVDs: Array[Byte], val ctx: LeadNodeExecutionContext,
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
    val rows = if (this.dvdRows != null) {
      import scala.collection.JavaConverters._
      dvdRows.asScala.map(dvdArr =>
        Row.fromSeq(dvdArr.map(org.apache.spark.sql.SnappySession.getValue(_, false))))
    } else {
       this.deserializedDVDs()
    }
    val catalog = session.sessionState.catalog
    val baseTableMetadata = catalog.getTableMetadata(ti)
    val schema = baseTableMetadata.schema
    val encoder = RowEncoder(schema)
   /* val internalRows = rows.map(encoder.toRow(_).copy)
    val localRDD = new RDD[InternalRow](session.sparkContext, Seq.empty) {
      def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
        internalRows.iterator
      protected def getPartitions: Array[Partition] = Array(new Partition {
        override def index: Int = 0
      })
    }
    val ds = session.internalCreateDataFrame(localRDD, schema) */
    val ds = session.internalCreateDataFrame(rows.map(encoder.toRow(_).copy()), schema)
    SparkSampleInsertExecuteImpl.insertIntoSampletables(ti, ds, catalog)
    // get sample tables tracked in catalog

    msg.lastResult(snappyResultHolder)
  }

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit = {
    DataSerializer.writeIntArray(null, out)
  }

  def deserializedDVDs(): Seq[Row] = {
    val baid = new ByteArrayDataInput()
    baid.initialize(this.serializedDVDs, null)
    val numEightColGroups = DataSerializer.readPrimitiveInt(baid);
    val numPartCols = DataSerializer.readPrimitiveByte(baid);
    val dvdTypes = DataSerializer.readIntArray(baid);
    val numCols = dvdTypes.length / 3
    val numRows = DataSerializer.readPrimitiveInt(baid);
    val dvdArray = Array.tabulate[DataValueDescriptor](numCols)(i => {
      SnappyResultHolder.getNewNullDVD(dvdTypes(i * 3), i, null,
        dvdTypes(i * 3 + 1), dvdTypes(i * 3 + 2), false)
    })
    for (i <- 0 until numRows) yield {
      dvdArray.foreach(_.setToNull())
      DVDIOUtil.readDVDArray(dvdArray, baid, numEightColGroups, numPartCols);
      Row.fromSeq(dvdArray.map(org.apache.spark.sql.SnappySession.getValue(_, false)))
    }
  }
}

object SparkSampleInsertExecuteImpl {
  def insertIntoSampletables(baseTable: TableIdentifier, ds: DataFrame,
    catalog: SnappySessionCatalog): Unit = {
    val aqpRelations = catalog.getSampleRelations(baseTable)
    aqpRelations.foreach {
      case (LogicalRelation(sr: SamplingRelation, _, _), _) => sr.insert(ds, false)
    }
  }
}