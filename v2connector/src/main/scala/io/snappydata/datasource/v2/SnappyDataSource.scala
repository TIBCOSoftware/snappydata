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

package io.snappydata.datasource.v2

import java.util.function.Supplier

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, SessionConfigSupport}

/**
 * DataSource V2 implementation for SnappyData
 */
class SnappyDataSource extends DataSourceV2 with
    ReadSupport with
    DataSourceRegister with
    SessionConfigSupport {


  /**
   * Creates a {@link DataSourceReader} to scan the data from this data source.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    validateOptions(options)
    new SnappyDataSourceReader(options)
  }

  override def shortName(): String = {
    V2Constants.DATASOURCE_SHORT_NAME
  }

  override def keyPrefix(): String = {
    V2Constants.KEY_PREFIX
  }

  private def validateOptions(options: DataSourceOptions): Unit = {
    options.get(V2Constants.SnappyConnection).
        orElseThrow(new Supplier[Throwable] {
          override def get(): Throwable =
            new IllegalArgumentException(
              s"Required configuration ${V2Constants.SnappyConnection} not specified")
        })

    options.get(V2Constants.TABLE_NAME).
        orElseThrow(new Supplier[Throwable] {
          override def get(): Throwable =
            new IllegalArgumentException(
              s"Required configuration ${V2Constants.TABLE_NAME} not specified")
        })
  }
}
