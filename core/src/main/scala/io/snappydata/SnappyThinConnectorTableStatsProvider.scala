/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package io.snappydata

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.{Timer, TimerTask}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.gemstone.gemfire.CancelException
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyIndexStats, SnappyRegionStats}
import io.snappydata.Constant._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

object SnappyThinConnectorTableStatsProvider extends TableStatsProviderService {

  private var conn: Connection = _
  private var getStatsStmt: PreparedStatement = _
  private var _url: String = _

  def initializeConnection(sparkContext: Option[SparkContext] = None): Unit = {
    val context = sparkContext match {
      case None => Option(SnappyContext.globalSparkContext)
      case _ => sparkContext
    }
    val securePart = context match {
      case Some(sc) =>
        Utils.getUserPassword(Utils.getInternalSparkConf(sc)) match {
          case None => ""
          case Some((user, password)) => s";user=$user;password=$password"
        }
      case None => ""
    }
    val jdbcOptions = new JDBCOptions(_url + securePart + ";route-query=false;", "",
      Map("driver" -> Constant.JDBC_CLIENT_DRIVER))
    conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    getStatsStmt = conn.prepareStatement("select * from sys.TABLESTATS")
  }

  def start(sc: SparkContext, url: String): Unit = {
    if (!doRun) {
      this.synchronized {
        if (!doRun) {
          _url = url
          initializeConnection(Some(sc))
          // reduce default interval a bit
          val delay = sc.getConf.getLong(Constant.SPARK_SNAPPY_PREFIX +
              "calcTableSizeInterval", DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL)
          doRun = true
          new Timer("SnappyThinConnectorTableStatsProvider", true).schedule(
            new TimerTask {
              override def run(): Unit = {
                try {
                  if (doRun) {
                    aggregateStats()
                  }
                } catch {
                  case _: CancelException => // ignore
                  case e: Exception => logError("SnappyThinConnectorTableStatsProvider", e)
                }
              }
            }, delay, delay)
        }
      }
    }
  }

  private def executeStatsStmt(sc: Option[SparkContext] = None): ResultSet = {
    if (conn == null) initializeConnection(sc)
    getStatsStmt.executeQuery()
  }

  private def closeConnection(): Unit = {
    val stmt = this.getStatsStmt
    if (stmt ne null) {
      try {
        stmt.close()
      } catch {
        case NonFatal(_) => // ignore
      }
      getStatsStmt = null
    }
    val c = this.conn
    if (c ne null) {
      try {
        c.close()
      } catch {
        case NonFatal(_) => // ignore
      }
      conn = null
    }
  }

  override def getStatsFromAllServers(sc: Option[SparkContext] = None): (Seq[SnappyRegionStats],
      Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = synchronized {
    try {
      val resultSet = executeStatsStmt(sc)
      val regionStats = new ArrayBuffer[SnappyRegionStats]
      while (resultSet.next()) {
        val tableName = resultSet.getString(1)
        val isColumnTable = resultSet.getBoolean(2)
        val isReplicatedTable = resultSet.getBoolean(3)
        val rowCount = resultSet.getLong(4)
        val sizeInMemory = resultSet.getLong(5)
        val totalSize = resultSet.getLong(6)
        val bucketCount = resultSet.getInt(7)
        regionStats += new SnappyRegionStats(tableName, totalSize, sizeInMemory, rowCount,
          isColumnTable, isReplicatedTable, bucketCount)
      }
      (regionStats, Nil, Nil)
    } catch {
      case NonFatal(e) =>
        logWarning("Warning: unable to retrieve table stats " +
            "from SnappyData cluster due to " + e.toString)
        logDebug("Exception stack trace: ", e)
        closeConnection()
        (Nil, Nil, Nil)
    }
  }

  override def stop(): Unit = {
    super.stop()
    closeConnection()
  }
}
