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
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import com.pivotal.gemfirexd.internal.snappy.LeadNodeExecutionContext

import org.apache.spark.Logging


class SparkSQLPreapreImpl(override val sql: String,
    override val schema: String,
    override val ctx: LeadNodeExecutionContext,
    senderVersion: Version) extends SparkSQLExecuteImpl(sql: String,
      schema: String,
      ctx: LeadNodeExecutionContext,
      senderVersion: Version, true, false, null) with Logging {

  override def packRows(msg: LeadNodeExecutorMsg,
      srh: SnappyResultHolder): Unit = {
    hdos.clearForReuse()

    DataSerializer.writeIntArray(Array[Int](1, StoredFormatIds.SQL_INTEGER_ID, -1, -1), hdos)

    msg.lastResult(srh)
  }

}
