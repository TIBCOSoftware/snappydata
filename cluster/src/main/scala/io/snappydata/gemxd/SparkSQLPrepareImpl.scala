/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdHeapDataOutputStream, SnappyResultHolder}
import com.pivotal.gemfirexd.internal.iapi.types.{DataType => _, _}
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.Logging
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.SnappySession.handleSubquery
import org.apache.spark.sql.catalyst.expressions.{ParamLiteral, ParamLiteralAtPrepare}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.util.SnappyUtils


class SparkSQLPrepareImpl(val sql: String,
    val schema: String,
    val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecute with Logging {

  if (Thread.currentThread().getContextClassLoader != null) {
    val loader = SnappyUtils.getSnappyStoreContextLoader(
      SparkSQLExecuteImpl.getContextOrCurrentClassLoader)
    Thread.currentThread().setContextClassLoader(loader)
  }

  private[this] val session = SnappySessionPerConnection
      .getSnappySessionForConnection(ctx.getConnId)

  session.setSchema(schema)

  session.setPreparedQuery(true, null)

  private[this] val analyzedPlan = session.prepareSQL(sql)

  private[this] val thresholdListener = Misc.getMemStore.thresholdListener()

  protected[this] val hdos = new GfxdHeapDataOutputStream(
    thresholdListener, sql, true, senderVersion)

  override def packRows(msg: LeadNodeExecutorMsg,
      srh: SnappyResultHolder): Unit = {
    hdos.clearForReuse()
    val paramLiteralAtPrepares = getAllParamLiteralsAtPrepare(analyzedPlan)
    if (paramLiteralAtPrepares != null) {
      val paramCount = paramLiteralAtPrepares.length
      val types = new Array[Int](paramCount * 4 + 1)
      types(0) = paramCount
      (0 until paramCount) foreach (i => {
        assert(paramLiteralAtPrepares(i).pos == i + 1)
        val index = i * 4 + 1
        val dType = paramLiteralAtPrepares(i).dataType
        val sqlType = getSQLType(dType)
        types(index) = sqlType._1
        types(index + 1) = sqlType._2
        types(index + 2) = sqlType._3
        types(index + 3) = if (paramLiteralAtPrepares(i).nullable) 1 else 0
      })
      DataSerializer.writeIntArray(types, hdos)
    } else {
      DataSerializer.writeIntArray(Array[Int](0), hdos)
    }

    if (msg.isLocallyExecuted) {
      SparkSQLExecuteImpl.handleLocalExecution(srh, hdos)
    }
    msg.lastResult(srh)
  }

  def getAllParamLiteralsAtPrepare(plan: LogicalPlan): Array[ParamLiteralAtPrepare] = {
    val res = new ArrayBuffer[ParamLiteralAtPrepare]()
    def allParams(plan: LogicalPlan) : LogicalPlan = plan transformAllExpressions {
      case p@ParamLiteralAtPrepare(_, _, _) => res += p
        p
    }
    handleSubquery(allParams(plan), allParams)
    res.toSet[ParamLiteralAtPrepare].toArray.sortBy(_.pos)
  }

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit =
    SparkSQLExecuteImpl.serializeRows(out, hasMetadata, hdos)

  // Also see SnappyResultHolder.getNewNullDVD(
  def getSQLType(dataType: DataType): (Int, Int, Int) = {
    dataType match {
      case IntegerType => (StoredFormatIds.SQL_INTEGER_ID, -1, -1)
      case StringType => (StoredFormatIds.SQL_CLOB_ID, -1, -1)
      case LongType => (StoredFormatIds.SQL_LONGINT_ID, -1, -1)
      case TimestampType => (StoredFormatIds.SQL_TIMESTAMP_ID, -1, -1)
      case DateType => (StoredFormatIds.SQL_DATE_ID, -1, -1)
      case DoubleType => (StoredFormatIds.SQL_DOUBLE_ID, -1, -1)
      case t: DecimalType => (StoredFormatIds.SQL_DECIMAL_ID,
          t.precision, t.scale)
      case FloatType => (StoredFormatIds.SQL_REAL_ID, -1, -1)
      case BooleanType => (StoredFormatIds.SQL_BOOLEAN_ID, -1, -1)
      case ShortType => (StoredFormatIds.SQL_SMALLINT_ID, -1, -1)
      case ByteType => (StoredFormatIds.SQL_TINYINT_ID, -1, -1)
      case BinaryType => (StoredFormatIds.SQL_BLOB_ID, -1, -1)
      case _: ArrayType | _: MapType | _: StructType =>
        // indicates complex types serialized as json strings
        (StoredFormatIds.REF_TYPE_ID, -1, -1)

      // send across rest as objects that will be displayed as json strings
      case _ => (StoredFormatIds.REF_TYPE_ID, -1, -1)
    }
  }
}
