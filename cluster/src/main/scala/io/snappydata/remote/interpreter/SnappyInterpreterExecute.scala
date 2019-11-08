/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata.remote.interpreter

import com.pivotal.gemfirexd.internal.snappy.InterpreterExecute
import io.snappydata.gemxd.SnappySessionPerConnection
import org.apache.spark.Logging
import org.apache.spark.sql.execution.InterpretCodeCommand

import scala.collection.mutable

class SnappyInterpreterExecute(sql: String, connId: Long) extends InterpreterExecute with Logging {

  override def execute(): Array[String] = {
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    val lp = session.sessionState.sqlParser.parsePlan(sql).asInstanceOf[InterpretCodeCommand]
    val interpreterHelper = SnappyInterpreterExecute.getOrCreateStateHolder(connId)
    interpreterHelper.interpret(Array(lp.code))
  }
}

object SnappyInterpreterExecute {
  val connToIntpHelperMap = new mutable.HashMap[Long, RemoteInterpreterStateHolder]()

  def getOrCreateStateHolder(connId: Long): RemoteInterpreterStateHolder = {
    connToIntpHelperMap.getOrElse(connId, {
        val stateholder = new RemoteInterpreterStateHolder(connId)
        connToIntpHelperMap.put(connId, stateholder)
        stateholder
    })
  }

  def closeRemoteInterpreter(connId: Long): Unit = {
   connToIntpHelperMap.get(connId) match {
     case Some(r) => r.close()
     case None => // Ignore. No interpreter got create for this session.
   }
  }
}
