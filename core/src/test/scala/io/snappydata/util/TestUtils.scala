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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SnappyContext, SnappySession}

object TestUtils extends Logging {

  def defaultCores: Int = math.min(8, Runtime.getRuntime.availableProcessors())

  def dropAllSchemas(session: SnappySession): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      val catalog = session.sessionCatalog
      catalog.setCurrentDatabase(catalog.defaultSchemaName)
      val skipSchemas = Seq(catalog.defaultSchemaName, "default", "sys", "sysibm")
      val userSchemas = catalog.listDatabases().filterNot(skipSchemas.contains)
      if (userSchemas.nonEmpty) {
        userSchemas.foreach { s =>
          try {
            session.sql(s"drop schema $s cascade")
          } catch {
            case t: Throwable =>
              if (!t.getMessage.contains("Cannot drop own schema")) {
                logError(s"Failure in dropping schema $s in cleanup", t)
              }
          }
        }
      }
      catalog.dropAllSchemaObjects(catalog.defaultSchemaName,
        ignoreIfNotExists = true, cascade = true)
      catalog.dropAllSchemaObjects("default", ignoreIfNotExists = true, cascade = true)
      catalog.clearTempTables()
    }
  }

  def resetAllFunctions(session: SnappySession): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null && !sc.isStopped) {
      try {
        val catalog = session.sessionState.catalog
        catalog.destroyAndRegisterBuiltInFunctionsForTests()
      } catch {
        case t: Throwable => logError("Failure in dropping function in cleanup", t)
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
