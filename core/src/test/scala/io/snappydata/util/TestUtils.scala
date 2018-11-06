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
package io.snappydata.util

import scala.collection.mutable

import _root_.com.gemstone.gemfire.cache.Region
import _root_.com.gemstone.gemfire.internal.cache.PartitionedRegion
import _root_.com.gemstone.gemfire.internal.shared.SystemProperties
import _root_.com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.hive.ExternalTableType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SnappyContext, SnappySession}

object TestUtils extends Logging {

  def defaultCores: Int = math.min(8, Runtime.getRuntime.availableProcessors())

  def dropAllTables(session: SnappySession): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      try {
        // drop all the stream tables that can have dependents at the end
        // also drop parents in colocated chain last (assuming chain length = 1)
        val ss = session.sessionState
        val streams = ss.catalog.getDataSourceTables(Seq(ExternalTableType.Stream))
        val samples = ss.catalog.getDataSourceTables(Seq(ExternalTableType.Sample))
        // Sample tables need to be dropped first as they depend on Base tables
        // for datasource resolution.
        // Temp fix. We need to add parent child relationship between them
        samples.foreach(s => session.dropTable(s.toString(), ifExists = true))

        val parents = mutable.HashSet[String]()

        val allTables = ss.catalog.getTables(None)

        // allows to skip dropping any tables not required to be dropped
        val skipPatterns = Seq("SYSIBM.", "SYS.", SystemProperties.SNAPPY_HIVE_METASTORE)

        val tablesToBeDropped = allTables.filter(x => {
          val tableName = x._1
          var allow = true
          for (skipPattern <- skipPatterns) {
            if (tableName.startsWith(skipPattern)) allow = false
          }
          allow
        })

        val allRegions = mutable.HashSet[String]()
        val allTablesWithRegions = tablesToBeDropped.map { t =>
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
        }.foreach(t => session.dropTable(t._1, ifExists = true))
        parents.foreach(session.dropTable(_, ifExists = true))
        streams.foreach(s => session.dropTable(s.toString(), ifExists = true))
      } catch {
        case t: Throwable => logError("Failure in dropping table in cleanup", t)
      }
    }
  }

  def dropAllFunctions(session: SnappySession): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      try {
        val catalog = session.sessionState.catalog
        catalog.listFunctions(Constant.DEFAULT_SCHEMA).map(_._1).foreach { func =>
          if (func.database.isDefined) {
            catalog.dropFunction(func, ignoreIfNotExists = false)
          } else {
            catalog.dropTempFunction(func.funcName, ignoreIfNotExists = false)
          }
        }

        catalog.clearTempTables()
        catalog.destroyAndRegisterBuiltInFunctions()

      } catch {
        case t: Throwable => logError("Failure in dropping function in cleanup", t)
      }

    }
  }

  def dropAllSchemas(session: SnappySession): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      val catalog = session.sessionCatalog
      val skipSchemas = Seq("APP", "DEFAULT", "SYS", "SYSIBM")
      val userSchemas = catalog.listDatabases().filterNot(s => skipSchemas.contains(s.toUpperCase))
      if (userSchemas.nonEmpty) {
        userSchemas.foreach { s =>
          try {
            session.sql(s"drop schema $s restrict")
          } catch {
            case t: Throwable => logError(s"Failure in dropping schema $s in cleanup", t)
          }
        }
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
      case _: Exception => false
    }
  }

  def projectColumns(row: Row, columnIndices: Array[Int], schema: StructType,
      convertToScalaRow: Boolean): GenericRow = {
    val ncols = columnIndices.length
    val newRow = new Array[Any](ncols)
    var index = 0
    if (convertToScalaRow) {
      while (index < ncols) {
        val colIndex = columnIndices(index)
        newRow(index) = CatalystTypeConverters.convertToScala(row(colIndex),
          schema(colIndex).dataType)
        index += 1
      }
    }
    else {
      while (index < ncols) {
        newRow(index) = row(columnIndices(index))
        index += 1
      }
    }
    new GenericRow(newRow)
  }
}
