/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.v2.connector.dummy.snappystore

// We need Unique package, where Spark looks for "DefaultSource"
import java.util.Optional
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types._


// DefaultSource checked by the Spark which implements the DataSourceV2
class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {

  def createReader(options: DataSourceOptions): DataSourceReader = {
    val url = options.get("url").orElse("")
    val driver = options.get("driver").orElse("")
    val size = options.get("size").orElse("")
    val tableName = options.get("tableName").orElse("")

    new SnappyDataSourceReader(tableName, url, driver, size)
  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode,
      options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new SnappyDataSourceWriter(options, mode, schema))
  }
}

