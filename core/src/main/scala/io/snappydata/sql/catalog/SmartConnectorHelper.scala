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
package io.snappydata.sql.catalog

import java.net.URL
import java.nio.file.{Files, Paths}
import java.sql.{CallableStatement, Connection, SQLException}

import scala.collection.mutable

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import io.snappydata.Constant
import io.snappydata.thrift.{CatalogMetadataDetails, CatalogMetadataRequest}

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.{SnappyContext, SnappySession, ThinClientConnectorMode}
import org.apache.spark.{Logging, SparkContext}

class SmartConnectorHelper(snappySession: SnappySession) extends Logging {

  private val conn: Connection = {
    val url = SnappyContext.getClusterMode(snappySession.sparkContext)
        .asInstanceOf[ThinClientConnectorMode].url
    val jdbcOptions = new JDBCOptions(url + getSecurePart + ";route-query=false;", "",
      Map("driver" -> Constant.JDBC_CLIENT_DRIVER))
    JdbcUtils.createConnectionFactory(jdbcOptions)()
  }

  private val getJarsStmtString = "call sys.GET_DEPLOYED_JARS(?)"

  private lazy val getCatalogMetaDataStmt: CallableStatement =
    conn.prepareCall("call sys.GET_CATALOG_METADATA(?, ?, ?)")

  private lazy val updateCatalogMetaDataStmt: CallableStatement =
    conn.prepareCall("call sys.UPDATE_CATALOG_METADATA(?, ?)")

  {
    val stmt = conn.prepareCall(getJarsStmtString)
    val sc = snappySession.sparkContext
    if ((sc ne null) && System.getProperty("pull-deployed-jars", "true").toBoolean) {
      try {
        executeGetJarsStmt(sc, stmt)
      } catch {
        case sqle: SQLException => logWarning(s"could not get jar and" +
            s" package information from snappy cluster", sqle)
      }
    }
  }

  private def getSecurePart: String = {
    var securePart = ""
    val user = snappySession.conf.get(Constant.SPARK_STORE_PREFIX + Attribute
        .USERNAME_ATTR, "")
    if (!user.isEmpty) {
      val pass = snappySession.conf.get(Constant.SPARK_STORE_PREFIX + Attribute
          .PASSWORD_ATTR, "")
      securePart = s";user=$user;password=$pass"
      logInfo(s"Using $user credentials to securely connect to snappydata cluster")
    }
    securePart
  }

  def setCurrentSchema(schema: String): Unit = {
    conn.setSchema(schema)
  }

  private def executeGetJarsStmt(sc: SparkContext, stmt: CallableStatement): Unit = {
    stmt.registerOutParameter(1, java.sql.Types.VARCHAR)
    stmt.execute()
    val jarsString = stmt.getString(1)
    var mutableList = new mutable.MutableList[URL]
    if (jarsString != null && jarsString.nonEmpty) {
      // comma separated list of file urls will be obtained
      jarsString.split(",").foreach(f => {
        mutableList.+=(new URL(f))
        val jarpath = f.substring(5)
        if (Files.isReadable(Paths.get(jarpath))) {
          try {
            sc.addJar(jarpath)
          } catch {
            // warn
            case ex: Exception => logWarning(s"could not add path $jarpath to SparkContext", ex)
          }
        } else {
          // May be the smart connector app does not care about the deployed jars or
          // the path is not readable so just log warning
          logWarning(s"could not add path $jarpath to SparkContext as the file is not readable")
        }
      })
      val newClassLoader = Utils.newMutableURLClassLoader(mutableList.toArray)
      Thread.currentThread().setContextClassLoader(newClassLoader)
    }
  }

  def getCatalogMetadata(operation: Int,
      request: CatalogMetadataRequest): CatalogMetadataDetails = {
    getCatalogMetaDataStmt.setInt(1, operation)
    val requestBytes = GemFireXDUtils.writeThriftObject(request)
    getCatalogMetaDataStmt.setBlob(2, new HarmonySerialBlob(requestBytes))
    getCatalogMetaDataStmt.registerOutParameter(3, java.sql.Types.BLOB)
    assert(!getCatalogMetaDataStmt.execute())
    val resultBlob = getCatalogMetaDataStmt.getBlob(3)
    val resultLen = resultBlob.length().toInt
    val result = new CatalogMetadataDetails()
    assert(GemFireXDUtils.readThriftObject(result, resultBlob.getBytes(1, resultLen)) == 0)
    resultBlob.free()
    result
  }

  def updateCatalogMetadata(operation: Int, request: CatalogMetadataDetails): Unit = {
    updateCatalogMetaDataStmt.setInt(1, operation)
    val requestBytes = GemFireXDUtils.writeThriftObject(request)
    updateCatalogMetaDataStmt.setBlob(2, new HarmonySerialBlob(requestBytes))
    assert(!updateCatalogMetaDataStmt.execute())
  }

  def close(): Unit = {
    try {
      getCatalogMetaDataStmt.close()
    } catch {
      case _: SQLException => // ignore
    }
    try {
      conn.close()
    } catch {
      case _: SQLException => // ignore
    }
  }
}
