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
import com.pivotal.gemfirexd.internal.snappy.{LeadNodeExecutionContext, SparkSQLExecute}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.ParamConstants
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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

  private[this] val df = session.sqlUncached(sql)

  private[this] val thresholdListener = Misc.getMemStore.thresholdListener()

  protected[this] val hdos = new GfxdHeapDataOutputStream(
    thresholdListener, sql, true, senderVersion)

  def allParamConstants(): Array[ParamConstants] = {
    var lls = new ArrayBuffer[ParamConstants]()
    df.queryExecution.analyzed transform {
      case q: LogicalPlan => q transformExpressionsUp {
        case pc@ParamConstants(_, _, _) =>
          lls += pc
          pc
      }
    }
    lls.sortBy(_.pos).toArray
  }

  override def packRows(msg: LeadNodeExecutorMsg,
      srh: SnappyResultHolder): Unit = {
    hdos.clearForReuse()
    val paramConstantsArr = allParamConstants()
    if (paramConstantsArr != null) {
      val paramCount = paramConstantsArr.length
      val types = new Array[Int](paramCount * 4 + 1)
      types(0) = paramCount
      (0 until paramCount) foreach (i => {
        val index = i * 4 + 1
        val dType = paramConstantsArr(i).dataType
        val sqlType = ParamConstants.getSQLType(dType)
        types(index) = sqlType._1
        types(index + 1) = sqlType._2
        types(index + 2) = sqlType._3
        types(index + 3) = if (paramConstantsArr(i).nullable) 1 else 0
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

  override def serializeRows(out: DataOutput, hasMetadata: Boolean): Unit =
    SparkSQLExecuteImpl.serializeRows(out, hasMetadata, hdos)
}
