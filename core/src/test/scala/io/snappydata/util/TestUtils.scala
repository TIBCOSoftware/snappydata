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

import io.snappydata.Constant
import io.snappydata.sql.catalog.CatalogObjectType._

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SnappyContext, SnappySession}

object TestUtils extends Logging {

  def defaultCores: Int = math.min(8, Runtime.getRuntime.availableProcessors())

  def dropAllTables(session: SnappySession): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      try {
        // drop all the stream tables that can have dependents at the end
        val catalog = session.externalCatalog
        val allTables = catalog.getAllTables()
        // collect views and dependents first
        if (allTables.isEmpty) return
        val (dependents, others) = allTables.partition(
          t => catalog.getBaseTable(t).isDefined || getTableType(t) == View)
        dependents.foreach(d => if (getTableType(d) == View) session.dropView(
          d.identifier.unquotedString, ifExists = true) else session.dropTable(
          d.identifier.unquotedString, ifExists = true))
        // drop streams at last
        val (streams, tables) = others.partition(getTableType(_) == Stream)
        tables.foreach(t => session.dropTable(t.identifier.unquotedString, ifExists = true))
        streams.foreach(s => session.dropTable(s.identifier.unquotedString, ifExists = true))
      } catch {
        case t: Throwable =>
          logError("Failure in dropping table in cleanup", t)
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
        catalog.destroyAndRegisterBuiltInFunctionsForTests()

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
            session.sql(s"drop schema $s")
          } catch {
            case t: Throwable => logError(s"Failure in dropping schema $s in cleanup", t)
          }
        }
      }
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
