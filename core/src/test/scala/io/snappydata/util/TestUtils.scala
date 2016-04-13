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
package io.snappydata.util

import _root_.com.gemstone.gemfire.cache.Region
import _root_.com.gemstone.gemfire.internal.cache.PartitionedRegion
import _root_.com.pivotal.gemfirexd.internal.engine.Misc

import scala.collection.mutable
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.hive.ExternalTableType

object TestUtils {

  def dropAllTables(snc: => SnappyContext): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      val snc = SnappyContext(sc)
      try {
        // drop all the stream tables that can have dependents at the end
        // also drop parents in colocated chain last (assuming chain length = 1)
        val streams = snc.catalog.getDataSourceTables(Seq(ExternalTableType.Stream))
        val parents = mutable.HashSet[String]()
        snc.catalog.getTables(None).filter { t =>
          if (streams.exists(_.toString() == t._1)) {
            false
          } else if (hasColocatedChildren(t._1)) {
            parents += t._1
            false
          } else true
        }.foreach(t => snc.dropTable(t._1, ifExists = true))
        parents.foreach(snc.dropTable(_, ifExists = true))
        streams.foreach(s => snc.dropTable(s.toString(), ifExists = true))
      } catch {
        case t: Throwable => t.printStackTrace()
      }
    }
  }

  def hasColocatedChildren(table: String): Boolean = {
    try {
      Misc.getRegionForTable(table, true).asInstanceOf[Region[_, _]] match {
        case pr: PartitionedRegion => pr.isColocatedBy
        case _ => false
      }
    } catch {
      case _: Exception => false
    }
  }
}
