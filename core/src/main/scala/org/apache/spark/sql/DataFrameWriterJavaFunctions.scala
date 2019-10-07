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
package org.apache.spark.sql

import org.apache.spark.sql.snappy.DataFrameWriterExtensions


class DataFrameWriterJavaFunctions(val dfWriter: DataFrameWriter[_]) {

  /**
   * Inserts the content of the [[DataFrame]] to the specified table. It requires
   * that the schema of the [[DataFrame]] is the same as the schema of the table.
   * If the row is already present then it is updated.
   *
   * This ignores all SaveMode
   */
  def putInto(tableName: String): Unit = {
    new DataFrameWriterExtensions(dfWriter).putInto(tableName)
  }

  def deleteFrom(tableName: String): Unit = {
    new DataFrameWriterExtensions(dfWriter).deleteFrom(tableName)
  }
}
