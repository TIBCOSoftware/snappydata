/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.sql.{CallableStatement, Connection}
import java.util.{Timer, TimerTask}

import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.{CancelException, DataSerializer}
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyIndexStats, SnappyRegionStats}
import io.snappydata.Constant._
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import scala.collection.JavaConverters._

object SnappyThinConnectorTableStatsProvider extends TableStatsProviderService {

  private var conn: Connection = null
  private var getStatsStmt: CallableStatement = null
  private var _url: String = null

  def initializeConnection(): Unit = {
    val jdbcOptions = new JDBCOptions(_url + ";route-query=false;", "",
      Map{"driver" -> "io.snappydata.jdbc.ClientDriver"})
    conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
    getStatsStmt = conn.prepareCall("call sys.GET_SNAPPY_TABLE_STATS(?)")
    getStatsStmt.registerOutParameter(1, java.sql.Types.BLOB)
  }

  def start(sc: SparkContext): Unit = {
    throw new IllegalStateException("This is expected to be called for " +
        "Embedded cluster mode only")
  }

  def start(sc: SparkContext, url: String): Unit = {
    this.synchronized {
      if (!doRun) {
        _url = url
        initializeConnection()
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

  def executeStatsStmt(): Unit = {
    if (conn == null) initializeConnection()
    getStatsStmt.execute()
  }

  override def getStatsFromAllServers: (Seq[SnappyRegionStats],
      Seq[SnappyIndexStats]) = {
    try {
      executeStatsStmt()
    } catch {
      case e: Exception =>
        logWarning("SnappyThinConnectorTableStatsProvider: exception while retrieving stats " +
            "from Snappy embedded cluster. Check whether the embedded cluster is stopped. " +
            "Exception: " + e.toString)
        logDebug("Exception stack trace: ", e)
        conn = null
        return (Seq.empty[SnappyRegionStats], Seq.empty[SnappyIndexStats])
    }
    val value = getStatsStmt.getBlob(1)
    val bdi: ByteArrayDataInput = new ByteArrayDataInput
    bdi.initialize(value.getBytes(1, value.length().asInstanceOf[Int]), null)
    val regionStats: java.util.List[SnappyRegionStats] =
    DataSerializer.readObject(bdi).asInstanceOf[java.util.ArrayList[SnappyRegionStats]]
    (regionStats.asScala, Seq.empty[SnappyIndexStats])
  }

}
