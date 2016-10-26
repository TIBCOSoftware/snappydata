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

package org.apache.spark.util

import java.io.File
import java.security.SecureClassLoader
import java.sql.{DriverManager, SQLException}

import _root_.io.snappydata.util.ServiceUtils
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext

object SnappyUtils {

  def getSnappyStoreContextLoader(parent: ClassLoader): ClassLoader = parent match {
    case _: SnappyContextLoader => parent // no double wrap
    case _ => new SnappyContextLoader(parent)
  }

  def installOrReplaceJar(jarName: String, jarPath: String, sc: SparkContext): Unit = {
    // for now using this approach as quickFix, later the add/replace should be called based on
    // whether the jar is already loaded or not.

    val jarExistsException =
      executeCall("CALL SQLJ.INSTALL_JAR_BYTES(?, ?)", jarName, jarPath, sc)
    if (jarExistsException) executeCall("CALL SQLJ.REPLACE_JAR_BYTES(?, ?)", jarName, jarPath, sc)
  }

  private def executeCall(sql: String, jarName: String, jarPath: String,
      sc: SparkContext): Boolean = {
    var jarExistsException = false
    val conn = DriverManager.getConnection(ServiceUtils.getLocatorJDBCURL(sc))
    try {
      val cs = conn.prepareCall(sql)
      cs.setBytes(1, FileUtils.readFileToByteArray(new File(jarPath)))
      cs.setString(2, jarName)
      cs.executeUpdate()
      cs.close()
    } catch {
      case se: SQLException => if (!se.getSQLState.equals("X0Y32")) throw se
      else jarExistsException = true
    }
    finally {
      conn.close()
    }

    jarExistsException
  }
}

class SnappyContextLoader(parent: ClassLoader)
    extends SecureClassLoader(parent) {

  @throws(classOf[ClassNotFoundException])
  override def loadClass(name: String): Class[_] = {
    try {
      parent.loadClass(name)
    } catch {
      case cnfe: ClassNotFoundException =>
        Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    }
  }
}
