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

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.engine.distributed.message.LeadNodeExecutorMsg
import com.pivotal.gemfirexd.internal.engine.distributed.SnappyResultHolder
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.LeadNodeExecutionContext

import org.apache.spark.Logging
import org.apache.spark.sql.execution.CachedPlanHelperExec


class SparkSQLPrepareImpl(override val sql: String,
    override val schema: String,
    override val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecuteImpl(sql: String,
      schema: String,
      ctx: LeadNodeExecutionContext,
      senderVersion: Version, true, true, null) with Logging {

  override def packRows(msg: LeadNodeExecutorMsg,
      srh: SnappyResultHolder): Unit = {
    hdos.clearForReuse()

    val paramConstants = CachedPlanHelperExec.allParamConstants()
    if (paramConstants != null) {
      val paramCount = paramConstants.length
      val types = new Array[Int](paramCount * 3 + 1)
      types(0) = paramCount
      (0 until paramCount) foreach (i => {
        val index = i * 3 + 1
        types(index) = StoredFormatIds.SQL_INTEGER_ID // paramConstants(i).value // TODO: type
        types(index + 1) = -1
        types(index + 2) = -1
      })
      DataSerializer.writeIntArray(types, hdos)
    } else {
      DataSerializer.writeIntArray(Array[Int](0), hdos)
    }
    
    msg.lastResult(srh)
  }

}
