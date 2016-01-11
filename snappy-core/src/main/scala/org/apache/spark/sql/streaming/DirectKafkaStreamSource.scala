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
package org.apache.spark.sql.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.util.Utils

/**
  * Created by ymahajan on 25/09/15.
  */
class DirectKafkaStreamSource extends SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              options: Map[String, String],
                              schema: StructType): BaseRelation = {
    new DirectKafkaStreamRelation(sqlContext, options, schema)
  }
}

case class DirectKafkaStreamRelation(@transient val sqlContext: SQLContext,
                                     options: Map[String, String],
                                     override val schema: StructType)
  extends StreamBaseRelation with Logging with StreamPlan with Serializable {

  @transient val context = StreamingCtxtHolder.streamingContext

  val storageLevel = options.get("storageLevel")
    .map(StorageLevel.fromString)
    .getOrElse(StorageLevel.MEMORY_AND_DISK_SER_2)

  private val streamToRows = {
    try {
      val clz = Utils.getContextOrSparkClassLoader.loadClass(options("streamToRows"))
      clz.newInstance().asInstanceOf[StreamToRowsConverter]
    } catch {
      case e: Exception => sys.error(s"Failed to load class : ${e.toString}")
    }
  }

  val topicsSet = options("topics").split(",").toSet
  val kafkaParams: Map[String, String] = options.get("kafkaParams").map { t =>
    t.split(", ").map { s =>
      val a = s.split("->")
      (a(0), a(1))
    }.toMap
  }.getOrElse(Map())

  // Currently works with Strings only
  @transient private val kafkaStream = KafkaUtils
    .createDirectStream[String, String, StringDecoder, StringDecoder](
    context, kafkaParams, topicsSet)
  // TODO Yogesh, need to provide typed decoders to createDirectStream


  @transient val stream: DStream[InternalRow] =
    kafkaStream.map(_._2).flatMap(streamToRows.toRows)
}
