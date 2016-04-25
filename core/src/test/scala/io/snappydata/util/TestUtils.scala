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

import scala.collection.mutable

import _root_.com.gemstone.gemfire.cache.Region
import _root_.com.gemstone.gemfire.internal.cache.PartitionedRegion
import _root_.com.pivotal.gemfirexd.internal.engine.Misc

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
        val samples = snc.catalog.getDataSourceTables(Seq(ExternalTableType.Sample))
        // Sample tables need to be dropped first as they depend on Base tables for datasource resolution
        // Temp fix. We need to add parent child relationship between them
        samples.foreach(s => snc.dropTable(s.toString(), ifExists = true))

        val parents = mutable.HashSet[String]()
        val allTables = snc.catalog.getTables(None)
        val allRegions = mutable.HashSet[String]()
        val allTablesWithRegions = allTables.map { t =>
          val table = t._1
          val tableName = if (table.indexOf('.') < 0) "APP." + table else table
          val path = Misc.getRegionPath(tableName)
          allRegions += path
          (table, path)
        }
        allTablesWithRegions.filter { case (table, path) =>
          if (streams.exists(_.toString() == table)) {
            false
          } else if (hasColocatedChildren(path, allRegions)) {
            parents += table
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

  private def checkColocatedByList(colocated: java.util.List[PartitionedRegion],
      allRegions: mutable.Set[String]): Boolean = {
    val itr = colocated.iterator()
    while (itr.hasNext) {
      if (allRegions.contains(itr.next().getFullPath)) {
        return true
      }
    }
    false
  }

  def hasColocatedChildren(path: String,
      allRegions: mutable.Set[String]): Boolean = {
    try {
      Misc.getRegion(path, true, false).asInstanceOf[Region[_, _]] match {
        case pr: PartitionedRegion => checkColocatedByList(pr.colocatedByList,
          allRegions)
        case _ => false
      }
    } catch {
      case e: Exception => false
    }
  }
}
