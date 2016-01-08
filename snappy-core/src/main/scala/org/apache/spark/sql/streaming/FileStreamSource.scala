/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.streaming

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils

/**
  * Created by ymahajan on 25/09/15.
  */
final class FileStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String],
                              schema: StructType): BaseRelation = {
    new FileStreamRelation(sqlContext, options, schema)
  }
}

case class FileStreamRelation(@transient val sqlContext: SQLContext,
                              options: Map[String, String],
                              override val schema: StructType)
  extends StreamBaseRelation with Logging with StreamPlan with Serializable {

  // HDFS directory to monitor for new file
  val DIRECTORY = "directory"

  // HDFS directory to monitor for new file
  val KEY = "key:"

  // Value type for reading HDFS file
  val VALUE = "value"

  // Input format for reading HDFS file
  val INPUT_FORMAT_HDFS = "inputformathdfs"

  // Function to filter paths to process
  val FILTER = "filter"

  // Should process only new files and ignore existing files in the directory
  val NEW_FILES_ONLY = "newfilesonly"

  // Hadoop configuration
  val CONF = "conf"

  val directory = options(DIRECTORY)

  @transient val context = StreamingCtxtHolder.streamingContext

  @transient val fileStream: DStream[String] = context.textFileStream(directory)
  // TODO: Yogesh, add support for other types of files streams

  private val streamToRows = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("streamToRows"))
      clz.newInstance().asInstanceOf[StreamToRowsConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  @transient val stream: DStream[InternalRow] = fileStream.flatMap(streamToRows.toRows)
}
