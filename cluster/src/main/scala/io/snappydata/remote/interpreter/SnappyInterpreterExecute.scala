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


package io.snappydata.remote.interpreter

import java.io.Serializable
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.pivotal.gemfirexd.Constants
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import com.pivotal.gemfirexd.internal.snappy.InterpreterExecute
import io.snappydata.Constant
import io.snappydata.gemxd.SnappySessionPerConnection
import org.apache.log4j.Logger

import org.apache.spark.Logging
import org.apache.spark.sql.execution.{GrantRevokeOnExternalTable, InterpretCodeCommand}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import scala.collection.JavaConverters._
import scala.collection.mutable

import com.pivotal.gemfirexd.internal.iapi.util.StringUtil


class SnappyInterpreterExecute(sql: String, connId: Long) extends InterpreterExecute with Logging {

  override def execute(user: String, authToken: String): AnyRef = {
    if (!SnappyInterpreterExecute.INITIALIZED) SnappyInterpreterExecute.init()
    val (allowed, group) = SnappyInterpreterExecute.permissions.isAllowed(user)
    if (Misc.isSecurityEnabled && !user.equalsIgnoreCase(SnappyInterpreterExecute.dbOwner)) {
      if (!allowed) {
        // throw exception
        throw StandardException.newException(SQLState.AUTH_NO_EXECUTE_PERMISSION, user,
          "scala code execution", "", "ComputeDB", "Cluster")
      }
    }
    val session = SnappySessionPerConnection.getSnappySessionForConnection(connId)
    val lp = session.sessionState.sqlParser.parseExec(sql).asInstanceOf[InterpretCodeCommand]
    val interpreterHelper = SnappyInterpreterExecute.getOrCreateStateHolder(connId, user,
      authToken, group)
    try {
      interpreterHelper.interpret(lp.code.split("\n"), lp.options)
    } finally {
      scala.Console.setOut(System.out)
    }
  }
}

object SnappyInterpreterExecute {

  private val intpRWLock = new ReentrantReadWriteLock()
  private val connToIntpHelperMap = new mutable.HashMap[
    Long, (String, String, RemoteInterpreterStateHolder)]

  private var permissions = new PermissionChecker

  private var INITIALIZED = false

  lazy val dbOwner = {
    Misc.getMemStore.getDatabase.getDataDictionary.getAuthorizationDatabaseOwner.toLowerCase()
  }

  def getLoader(prop: String): ClassLoader = {
    val x = connToIntpHelperMap.find(x => x._2._3.replOutputDirStr.equals(prop))
    if (x.isDefined) {
      return x.get._2._3.intp.classLoader
    }
    null
  }

  def handleNewPermissions(grantor: String, isGrant: Boolean, users: String): Unit = {
    if (!Misc.isSecurityEnabled) return
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      val dbOwner = SnappyInterpreterExecute.dbOwner
      if (!grantor.toLowerCase.equals(dbOwner)) {
        throw StandardException.newException(
          SQLState.AUTH_NO_OBJECT_PERMISSION, grantor,
          "grant/revoke of scala code execution", "ComputeDB", "Cluster")
      }
      val commaSepVals = users.split(",")
      commaSepVals.foreach(u => {
        val uUC = StringUtil.SQLToUpperCase(u)
        if (isGrant) {
          if (uUC.startsWith(Constants.LDAP_GROUP_PREFIX)) permissions.addLdapGroup(uUC)
          else permissions.addUser(u)
        } else {
          if (uUC.startsWith(Constants.LDAP_GROUP_PREFIX)) removeAGroupAndCleanup(uUC)
          else removeAUserAndCleanup(u)
        }
      })
      updatePersistentState
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
  }

  private def removeAUserAndCleanup(user: String): Unit = {
    permissions.removeUser(user)
    val toBeCleanedUpEntries = connToIntpHelperMap.filter(
      x => x._2._1.isEmpty && user.equalsIgnoreCase(x._2._2))
    toBeCleanedUpEntries.foreach(x => {
      connToIntpHelperMap.remove(x._1)
      x._2._3.close()
    })
  }

  private def removeAGroupAndCleanup(group: String): Unit = {
    permissions.removeLdapGroup(group)
    val toBeCleanedUpEntries = connToIntpHelperMap.filter(
      x => group.equalsIgnoreCase(x._2._1)
    )
    toBeCleanedUpEntries.foreach(x => {
      connToIntpHelperMap.remove(x._1)
      x._2._3.close()
    })
  }

  def refreshOnLdapGroupRefresh(group: String): Unit = {
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      permissions.refreshOnLdapGroupRefresh(group)
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
    // TODO (Optimization) Reuse the grantees list retrieved above in below method.
    updateMetaRegion(group)
  }

  private def updateMetaRegion(group: String): Unit = {
    val r = Misc.getMemStore.getMetadataCmdRgn
    val allAuthKeys = r.keySet().asScala.filter(s =>
      s.startsWith(GrantRevokeOnExternalTable.META_REGION_KEY_PREFIX))
    allAuthKeys.foreach(k => {
      val p = r.get(k)
      if (p != null) {
        p.asInstanceOf[PermissionChecker].addLdapGroup(getNameWithLDAPPrefix(group), true)
      }
    })
  }

  private def getNameWithLDAPPrefix(g: String): String = {
    val gUC = StringUtil.SQLToUpperCase(g)
    if (!gUC.startsWith(Constants.LDAP_GROUP_PREFIX)) s"${Constants.LDAP_GROUP_PREFIX}$g"
    else g
  }

  private def updatePersistentState = {
    Misc.getMemStore.getMetadataCmdRgn.put(Constant.GRANT_REVOKE_KEY, permissions)
  }

  def getOrCreateStateHolder(connId: Long,
      user: String, authToken: String, group: String): RemoteInterpreterStateHolder = {
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      connToIntpHelperMap.getOrElse(connId, {
        val stateholder = new RemoteInterpreterStateHolder(connId, user, authToken)
        connToIntpHelperMap.put(connId, (group, user, stateholder))
        (group, user, stateholder)
      })._3
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
  }

  def closeRemoteInterpreter(connId: Long): Unit = {
    var lockTaken = false
    try {
      lockTaken = intpRWLock.writeLock().tryLock()
      connToIntpHelperMap.get(connId) match {
        case Some(r) => r._3.close()
          connToIntpHelperMap.remove(connId)
        case None => // Ignore. No interpreter got create for this session.
      }
    } finally {
      if (lockTaken) intpRWLock.writeLock().unlock()
    }
  }

  private def init(): Unit = {
    val key = Constant.GRANT_REVOKE_KEY
    val permissionsObj = Misc.getMemStore.getMetadataCmdRgn.get(key)
    if (permissionsObj != null) {
      permissions = permissionsObj.asInstanceOf[PermissionChecker]
    } else {
      permissions = new PermissionChecker
    }
    INITIALIZED = true
  }

  private class PermissionChecker extends Serializable {
    private val groupToUsersMap: mutable.Map[String, List[String]] = new mutable.HashMap
    private val allowedUsers: mutable.ListBuffer[String] = new mutable.ListBuffer[String]

    def isAllowed(user: String): (Boolean, String) = {
      if (allowedUsers.contains(user)) return (true, "")
      for ((group, list) <- groupToUsersMap) {
        if (list.contains(user)) return (true, group)
      }
      (false, "")
    }

    def addUser(user: String): Unit = {
      if (!allowedUsers.contains(user)) allowedUsers += user
    }

    def removeUser(toBeRemovedUser: String): Unit = {
      if (allowedUsers.contains(toBeRemovedUser)) allowedUsers -= toBeRemovedUser
    }

    def addLdapGroup(group: String, updateOnly: Boolean = false): Unit = {
      if (updateOnly && !groupToUsersMap.contains(group)) return
      val grantees = ExternalStoreUtils.getExpandedGranteesIterator(Seq(group)).filterNot(
        _.startsWith(Constants.LDAP_GROUP_PREFIX)).map(_.toLowerCase).toList
      groupToUsersMap += (group -> grantees)
    }

    def removeLdapGroup(toBeRemovedGroup: String): Unit = {
      groupToUsersMap.remove(toBeRemovedGroup)
    }

    def refreshOnLdapGroupRefresh(group: String): Unit = {
      val grantees = ExternalStoreUtils.getExpandedGranteesIterator(Seq(
        getNameWithLDAPPrefix(group))).toList
      groupToUsersMap.put(group, grantees)
    }
  }

  object PermissionChecker {

    private val logger = Logger.getLogger(
      "io.snappydata.remote.interpreter.SnappyInterpreterExecute")

    def isAllowed(key: String, currentUser: String, tableSchema: String): Boolean = {
      if (currentUser.equalsIgnoreCase(tableSchema) || currentUser.equalsIgnoreCase(dbOwner)) {
        return true
      }

      val permissionsObj = Misc.getMemStore.getMetadataCmdRgn.get(key)
      if (permissionsObj == null) return false
      permissionsObj.asInstanceOf[PermissionChecker].isAllowed(currentUser)._1
    }

    def addRemoveUserForKey(key: String, isGrant: Boolean, users: String): Unit = {
      PermissionChecker.synchronized {
        val permissionsObj = Misc.getMemStore.getMetadataCmdRgn.get(key)
        val permissions = if (permissionsObj != null) permissionsObj.asInstanceOf[PermissionChecker]
        else new PermissionChecker
        // expand the users list. Can be a mix of normal user and ldap group
        val commaSepVals = users.split(",")
        commaSepVals.foreach(u => {
          val uUC = StringUtil.SQLToUpperCase(u)
          if (isGrant) {
            if (uUC.startsWith(Constants.LDAP_GROUP_PREFIX)) permissions.addLdapGroup(uUC)
            else permissions.addUser(u)
          } else {
            if (uUC.startsWith(Constants.LDAP_GROUP_PREFIX)) permissions.removeLdapGroup(uUC)
            else permissions.removeUser(u)
          }
        })
        logger.debug(s"Putting permission obj = $permissions against key = $key")
        Misc.getMemStore.getMetadataCmdRgn.put(key, permissions)
      }
    }
  }
}
