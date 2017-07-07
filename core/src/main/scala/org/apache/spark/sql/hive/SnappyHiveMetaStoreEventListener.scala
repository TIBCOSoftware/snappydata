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
package org.apache.spark.sql.hive

import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.execute.FunctionService
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdMessage, SnappyRemoveCachedObjectsFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.MetaStoreEventListener
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.events.DropTableEvent

import org.apache.spark.Logging
import org.apache.spark.sql._

class SnappyHiveMetaStoreEventListener(config: Configuration)
    extends MetaStoreEventListener(config) with Logging {

  /**
   * Callback function invoked when a table is dropped from Hive Metastore.
   * In a split cluster deployment, this cleans up any cached objects in
   * embedded mode cluster when a table is dropped from an external cluster
   * and vice-a-versa
   */
  @throws(classOf[MetaException])
  override def onDropTable(tableEvent: DropTableEvent): Unit = {
    try {
      handleDropTableEvent(tableEvent)
    } catch {
      case NonFatal(e) =>
        log.error("onDropTable: exception ", e)
    }
  }

  private def handleDropTableEvent(tableEvent: DropTableEvent): Unit = {
    SnappyContext.
        getClusterMode(SnappyContext.globalSparkContext) match {
      case SnappyEmbeddedMode(_, _) =>
        // table is dropped on embedded lead so send message to external
        // cluster driver
        val accessors = GemFireXDUtils.getGfxdAdvisor.adviseAccessors(null)
        accessors.remove(Misc.getMyId)
        if (accessors.size > 0) {
          val table = getTableName(tableEvent)
          log.debug(s"onDropTable invoked for $table")
          val args = SnappyRemoveCachedObjectsFunction.
              newArgs(table, false)
          FunctionService.onMembers(accessors).withArgs(args).
              execute(SnappyRemoveCachedObjectsFunction.ID)
        }

      case SplitClusterMode(_, _) =>
        // table is dropped on external cluster so send message to
        // embedded mode nodes
        val otherMembers = GfxdMessage.getOtherMembers
        if (otherMembers.size > 0) {
          val table = getTableName(tableEvent)
          log.debug(s"onDropTable invoked for $table")
          val args = SnappyRemoveCachedObjectsFunction.
              newArgs(table, true)
          FunctionService.onMembers(otherMembers).withArgs(args).
              execute(SnappyRemoveCachedObjectsFunction.ID)
        }

      case _ => // do nothing
    }
  }

  private def getTableName(tableEvent: DropTableEvent): String = {
    val sqlConf = SparkSession.getActiveSession.get.sqlContext.conf
    val table = tableEvent.getTable.getDbName + "." +
        tableEvent.getTable.getTableName
    SnappyStoreHiveCatalog.processTableIdentifier(table, sqlConf)
  }
}
