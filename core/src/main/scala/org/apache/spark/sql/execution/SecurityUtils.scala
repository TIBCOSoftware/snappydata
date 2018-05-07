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

package org.apache.spark.sql.execution

import java.sql.SQLException

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer
import com.pivotal.gemfirexd.internal.impl.jdbc.{EmbedConnection, TransactionResourceImpl}

import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.sources.ConnectionProperties

/**
 * Common security related calls.
 */
object SecurityUtils {

  /**
   * Authorize a column/row table operation.
   *
   * @param rowBufferTable fully qualified name of the row buffer table
   * @param projection     columns being read/written (or Array.emptyIntArray)
   * @param authType       one of *_PRIV authorization types in [[Authorizer]]
   * @param opType         one of *_OP operation types in [[Authorizer]]
   * @param connProperties connection properties to use for the operation
   * @param forExecutor    whether current node type is an executor or driver
   */
  @throws[SQLException]
  def authorizeTableOperation(rowBufferTable: String, projection: Array[Int],
      authType: Int, opType: Int, connProperties: ConnectionProperties,
      forExecutor: Boolean = true): Unit = {
    if (Misc.isSecurityEnabled) {
      // pool connection is a proxy so get embedded connection from statement
      val pooledConnection = ExternalStoreUtils.getConnection(rowBufferTable,
        connProperties, forExecutor)
      val stmt = pooledConnection.createStatement()
      val conn = stmt.getConnection.asInstanceOf[EmbedConnection]
      stmt.close()
      val lcc = conn.getLanguageConnectionContext
      var popContext = false
      try {
        conn.getTR.setupContextStack()
        lcc.pushMe()
        popContext = true
        GfxdSystemProcedures.authorizeTableOperation(lcc, rowBufferTable,
          projection, Authorizer.SELECT_PRIV, Authorizer.SQL_SELECT_OP)
      } catch {
        case t: Throwable => throw TransactionResourceImpl.wrapInSQLException(t)
      } finally {
        try {
          if (popContext) {
            lcc.popMe()
            conn.getTR.restoreContextStack()
          }
        } finally {
          // Since it is a pooled connection, the  underlying embed connection
          // should not be closed, instead pooled connection should be closed,
          // so that connection pool is not exhausted
          pooledConnection.commit()
          pooledConnection.close()
        }
      }
    }
  }
}
