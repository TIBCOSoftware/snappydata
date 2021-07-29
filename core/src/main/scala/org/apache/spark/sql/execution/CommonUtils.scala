/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.execution

import scala.concurrent.duration.Duration
import scala.concurrent.{Awaitable, ExecutionContext}

import io.snappydata.Constant

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.types.{CharType, DataType, MetadataBuilder, StringType, StructType, VarcharType}
import org.apache.spark.util.ThreadUtils


object CommonUtils {
  def modifySchemaIfNeeded(options: Map[String, String], session: SnappySession,
    schema: StructType): StructType = {
    options.get(Constant.STRING_TYPE_AS).map(stringTypeAs => {
      val parser = session.snappyParser.newInstance()
      val stringAsDataType = parser.parseSQLOnly[DataType](stringTypeAs,
        parser.columnDataType.run())
      val (dataTypeStr, size) = stringAsDataType match {
        case CharType(size) => "CHAR" -> size
        case VarcharType(Int.MaxValue) => "CLOB" -> Int.MaxValue
        case VarcharType(size) => "VARCHAR" -> size
        case StringType => "STRING" -> -1
      }

      StructType(schema.map(sf => if (sf.dataType.equals(StringType)) {
        val oldMetadata = sf.metadata
        val builder = new MetadataBuilder()
        if ((oldMetadata eq null) ||
          !oldMetadata.contains(Constant.CHAR_TYPE_BASE_PROP) ||
          /* oldMetadata.getString(Constant.CHAR_TYPE_BASE_PROP).
            equalsIgnoreCase("CLOB")  || */
          oldMetadata.getString(Constant.CHAR_TYPE_BASE_PROP).
            equalsIgnoreCase("STRING")) {
          val newMetadata = if (oldMetadata eq null) {
            builder.putString(Constant.CHAR_TYPE_BASE_PROP, dataTypeStr).
              putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
            builder.build()
          } else {
            builder.withMetadata(oldMetadata).
              putString(Constant.CHAR_TYPE_BASE_PROP, dataTypeStr).
              putLong(Constant.CHAR_TYPE_SIZE_PROP, size).build()
          }
          sf.copy(metadata = newMetadata)
        } else {
          sf
        }
      } else {
        sf
      }))
    }).getOrElse(schema)
  }

  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T =
    ThreadUtils.awaitResult(awaitable, atMost)

  val waiterExecutionContext: ExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("snappydata-waiter", 128))
}
