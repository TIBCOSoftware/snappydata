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

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.internal.snappy.InterpreterExecute
import io.snappydata.Constant
import io.snappydata.gemxd.SnappySessionPerConnection
import org.apache.spark.Logging
import org.apache.spark.sql.execution.InterpretCodeCommand

import scala.collection.mutable

class SnappyInterpreterExecute(sql: String, connId: Long) extends InterpreterExecute with Logging {

  override def execute(user: String): Array[String] = {
    if (!SnappyInterpreterExecute.INITIALIZED) SnappyInterpreterExecute.init()
    if (Misc.isSecurityEnabled && !user.equalsIgnoreCase(SnappyInterpreterExecute.dbOwner)) {
      if (!SnappyInterpreterExecute.allowedUsers.contains(user)) {
        // throw exception
        throw StandardException.newException(
          SQLState.AUTH_NO_EXECUTE_PERMISSION, user, "intp", "", "ComputeDB", "Cluster")
      }
    }
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    val lp = session.sessionState.sqlParser.parsePlan(sql).asInstanceOf[InterpretCodeCommand]
    val interpreterHelper = SnappyInterpreterExecute.getOrCreateStateHolder(connId, user)
    interpreterHelper.interpret(Array(lp.code))
  }
}

object SnappyInterpreterExecute {

  val intpRWLock = new ReentrantReadWriteLock()
  val connToIntpHelperMap = new mutable.HashMap[Long, (String, RemoteInterpreterStateHolder)]
  var allowedUsers: Set[String] = Set()

  var INITIALIZED = false

  lazy val dbOwner = {
    Misc.getMemStore.getDatabase.getDataDictionary.getAuthorizationDatabaseOwner.toLowerCase()
  }

  def getOrCreateStateHolder(connId: Long, user: String): RemoteInterpreterStateHolder = {
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      connToIntpHelperMap.getOrElse(connId, {
        val stateholder = new RemoteInterpreterStateHolder(connId)
        connToIntpHelperMap.put(connId, (user, stateholder))
        (user, stateholder)
      })._2
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
  }

  def closeRemoteInterpreter(connId: Long): Unit = {
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      connToIntpHelperMap.get(connId) match {
        case Some(r) => r._2.close()
          connToIntpHelperMap.remove(connId)
        case None => // Ignore. No interpreter got create for this session.
      }
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
  }

  def init(): Unit = {
    val key = Constant.GRANT_REVOKE_KEY
    val allowedIntpUsers = Misc.getMemStore.getMetadataCmdRgn.get(key)
    if (allowedIntpUsers != null)
      allowedUsers = allowedIntpUsers.split(",").map(_.toLowerCase).toSet
    INITIALIZED = true
  }

  def doCleanupAsPerNewGrantRevoke(currentAllowedUsers: Set[String]): Unit = {
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      val notAllowed = connToIntpHelperMap.filterNot(x => currentAllowedUsers.contains(x._2._1))
      for ((id, (_, holder)) <- notAllowed) {
        connToIntpHelperMap.remove(id)
        holder.close()
      }
      allowedUsers = currentAllowedUsers
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
  }
}
